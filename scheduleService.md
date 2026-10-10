# Schedule Service

Service กลางสำหรับตั้งเวลาและรอบการทำงาน (cron/job) ของทุก service ในระบบ · serviceKey: `schedule`

service อื่นไม่ต้องมี cron ของตัวเอง แต่ฝาก "นัดหมาย" ไว้ที่ schedule ผ่าน Kafka topic `set-schedule` เมื่อถึงเวลา schedule จะส่ง event `schedule-alarm` กลับไปหา service เจ้าของนัดหมาย

ทำไมต้องรวมไว้ที่เดียว: ถ้าแต่ละ service ตั้ง cron เอง เมื่อ scale หลาย replica ทุก replica จะยิงงานเดียวกันซ้ำ · เมื่อใช้ schedule กลาง แต่ละ service รับ `schedule-alarm` ผ่าน consumer group ของตัวเอง Kafka จึงส่งแต่ละ alarm ให้ replica เดียวในกลุ่ม ⇒ service ปลายทางรันหลาย replica ได้โดยไม่ยิงซ้ำ

ตัว schedule service เองออกแบบให้รัน **replica เดียว** (single instance)

<br>

- [ภาพรวมการใช้งาน](#ภาพรวมการใชงาน)
- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [kafka produce Reference](#kafka-produce-reference)

---

<br>
<br>

## ภาพรวมการใช้งาน

```
service X ──set-schedule {action: ADD | REMOVE, schedule}──────────────▶ schedule
schedule  ──set-schedule-result {action: SUCCESS | ERROR}──────────────▶ service X
[ตรวจทุก 1 นาที] หา schedule ที่ถึงเวลา → คำนวณรอบถัดไป → บันทึกประวัติ (transaction)
schedule  ──schedule-alarm {action: ADD, scheduleAlarm}  header.serviceKey = X ──▶ service X
service X ──schedule-alarm-result {action: ADD, scheduleAlarm + เวลารับ/ส่ง}──▶ schedule
```

- header ของทุกข้อความมี `appKey` และ `serviceKey` · `serviceKey` = **service ปลายทาง** เสมอ
  - ส่ง `set-schedule` / `schedule-alarm-result` มาที่ schedule ⇒ header `serviceKey: schedule`
  - schedule ส่ง `schedule-alarm` ⇒ header `serviceKey` = serviceKey ของ service เจ้าของนัดหมาย
- ใน payload `schedule.serviceKey` = service ที่จะได้รับ alarm (ปกติคือ serviceKey ของผู้ส่งเอง)
- ความละเอียดของเวลา 1 นาที
- ใช้ `scheduleRefKey` / `scheduleRefType` อ้างถึงข้อมูลฝั่งตัวเอง และ `alarmData` ฝากข้อมูลอิสระ (object) ที่จะได้คืนมาตอน alarm
- ประวัติการส่งเก็บเป็น ScheduleTransaction จับคู่กับผลตอบกลับด้วย `alarmRefKey`

---

## API Reference
---

GraphQL endpoint `/graphql` · ทุก API ต้อง login (header `authorization`) และมี permission ตามที่ระบุ ยกเว้น API แบบ `...ByAppKey` ที่ใช้ app credential ของระบบภายนอก · การระบุ `appKey` อื่นนอกจากแอปที่ login ต้องมี permission `systemApp`

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

#### Get Schedules

ดึงรายการ schedule ทั้งหมดของแอปที่ login (แบ่งหน้า) · permission: `getSchedules`

```graphql
query {
  getSchedules(input: {
    filter: { serviceKey: "storage", isActive: true }
    search: { title: "แจ้งเตือน" }
    sort: { sortBy: createdAt, sortOrder: DESC }
    pagination: { limit: 10, page: 1 }
  }) {
    schedules { _id scheduleKey serviceKey title isRecurring nextNotificationAt isSent }
    pagination { page limit totalItems totalPages hasNextPage }
  }
}
```

Input `GetScheduleInput`

| key | Type | คำอธิบาย |
| --- | --- | --- |
| filter | GetScheduleFilterInput | กรองตรงตัว: serviceKey, scheduleKey, scheduleRefKey, scheduleRefType, title, description, isActive, timeZone, isRecurring, notificationCount, isSent, createdBy, updatedBy |
| search | GetScheduleSearchInput | ค้นหาบางส่วน: serviceKey, scheduleKey, scheduleRefKey, scheduleRefType, title, description |
| sort | GetScheduleSortInput | `sortBy` (ค่าเริ่มต้น `createdAt`), `sortOrder` ASC / DESC |
| pagination | CustomPaginateInput | `limit` (ค่าเริ่มต้น 10), `page` (ค่าเริ่มต้น 1) |

Response : `SchedulePagination` = `{ schedules: [Schedule], pagination: CustomPaginate }`

Schedule

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| serviceKey | String | service ที่จะได้รับ alarm |
| scheduleKey | String | key อ้างอิงของ schedule |
| scheduleRefKey | String | key อ้างอิงข้อมูลฝั่ง service เจ้าของ |
| scheduleRefType | String | ประเภทข้อมูลฝั่ง service เจ้าของ |
| alarmData | JSON | ข้อมูลอิสระที่ฝากไว้ จะส่งคืนตอน alarm |
| title | String | ชื่อ schedule |
| description | String | รายละเอียด |
| isActive | Boolean | เปิดใช้งานอยู่หรือไม่ |
| timeZone | String | เขตเวลา เช่น `Asia/Bangkok` (ใช้กับ recurrence แบบ `SPECIFIC`) |
| isRecurring | Boolean | วนซ้ำหรือไม่ |
| recurrence | ScheduleRecurrence | การตั้งค่าวนซ้ำ (ดูด้านล่าง) |
| firstNotificationAt | Date | เวลาแจ้งเตือนครั้งแรก |
| nextNotificationAt | Date | เวลาแจ้งเตือนครั้งถัดไป |
| notificationCount | Int | จำนวนครั้งที่แจ้งเตือนแล้ว |
| isSent | Boolean | ส่งครบแล้วหรือไม่ |
| expiryAt | Date | เวลาหมดอายุ |
| lastSentAt | Date | เวลาส่งครั้งล่าสุด |
| createdBy, updatedBy | String | ผู้สร้าง / ผู้แก้ไข |
| createdAt, updatedAt | Date | เวลาสร้าง / แก้ไขล่าสุด |

ScheduleRecurrence

| key | Type | คำอธิบาย |
| --- | --- | --- |
| type | ENUM | `INTERVAL` (ทุก ๆ ช่วงเวลา) · `SPECIFIC` (วันที่กำหนด) |
| interval | ENUM | `MINUTES`, `HOURS`, `DAYS`, `WEEKS`, `MONTHS` (ใช้กับ `INTERVAL`) |
| value | Int | จำนวนหน่วยของ interval เช่น 24 + `HOURS` |
| specific.dayOfWeek | Int | 0–6 (อาทิตย์ = 0) ใช้กับ `SPECIFIC` |
| specific.dayOfMonth | Int | 1–31 ใช้กับ `SPECIFIC` |

---

#### Get Schedule By ID

ดึง schedule ตาม `_id` · permission: `getScheduleById`

```graphql
query {
  getScheduleById(scheduleId: "6650f0c2a1b2c3d4e5f60718") {
    _id scheduleKey title nextNotificationAt recurrence { type interval value }
  }
}
```

Response : `Schedule`

---

#### Get Schedule By Key

ดึง schedule ตาม `scheduleKey` · permission: `getScheduleByKey`

```graphql
query {
  getScheduleByKey(scheduleKey: "aB3dE5fG7h") { _id title nextNotificationAt isSent }
}
```

Response : `Schedule`

---

#### Get Schedule By AppKey

ดึงรายการ schedule ของแอปที่ระบุ สำหรับระบบภายนอกที่เรียกด้วย app credential (header `X-APP-CLIENT-ID` + `X-APP-CLIENT-SECRET`)

```graphql
query {
  getScheduleByAppKey(appKey: "yourAppKey", input: { pagination: { limit: 10, page: 1 } }) {
    schedules { _id title nextNotificationAt }
    pagination { totalItems }
  }
}
```

Response : `SchedulePagination`

---

#### Get Schedule Transactions

ดึงประวัติการส่ง alarm ของแอปที่ login (แบ่งหน้า) · permission: `getScheduleTransactions`

```graphql
query {
  getScheduleTransactions(input: {
    filter: { isSuccess: false }
    sort: { sortBy: scheduleSentAt, sortOrder: DESC }
    pagination: { limit: 20, page: 1 }
  }) {
    scheduleTransaction { _id scheduleKey alarmRefKey scheduleSentAt clientReceivedAt isSuccess }
    pagination { totalItems }
  }
}
```

Input `GetScheduleTransactionInput`: `filter` (scheduleKey, isSuccess, alarmRefKey, createdBy, updatedBy, schedule) · `search` (scheduleKey, alarmRefKey) · `sort` · `pagination`

Response : `ScheduleTransactionPagination` = `{ scheduleTransaction: [ScheduleTransaction], pagination: CustomPaginate }`

ScheduleTransaction

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| scheduleId | ID | id ของ schedule |
| scheduleKey | String | key ของ schedule |
| alarmRefKey | String | key ของการส่งรอบนี้ ใช้จับคู่กับ `schedule-alarm-result` |
| scheduleSentAt | Date | เวลาที่ schedule ส่ง alarm |
| clientReceivedAt | Date | เวลาที่ service ปลายทางได้รับ |
| clientSentAt | Date | เวลาที่ service ปลายทางตอบกลับ |
| scheduleReceivedAt | Date | เวลาที่ schedule ได้รับผลตอบกลับ |
| isSuccess | Boolean | ได้รับผลตอบกลับแล้วหรือไม่ |
| schedule | Schedule | ข้อมูล schedule ณ เวลาที่ส่ง |
| createdAt, updatedAt | Date | |

---

#### Get Schedule Transaction By ID

ดึงประวัติการส่งตาม `_id` · permission: `getScheduleTransactionById`

```graphql
query {
  getScheduleTransactionById(scheduleTransactionId: "6650f0c2a1b2c3d4e5f60719") {
    _id alarmRefKey isSuccess scheduleSentAt scheduleReceivedAt
  }
}
```

Response : `ScheduleTransaction`

---

#### Get Schedule Transactions By AppKey

ดึงประวัติการส่งของแอปที่ระบุ สำหรับระบบภายนอกที่เรียกด้วย app credential

```graphql
query {
  getScheduleTransactionsByAppKey(appKey: "yourAppKey", input: { pagination: { limit: 20, page: 1 } }) {
    scheduleTransaction { _id alarmRefKey isSuccess }
  }
}
```

Response : `ScheduleTransactionPagination`

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

#### Create Schedule

สร้าง schedule จากหน้าบ้าน (ผลเหมือนส่ง `set-schedule` action `ADD`) · permission: `createSchedule`

```graphql
mutation {
  createSchedule(input: {
    serviceKey: "yourServiceKey"
    scheduleRefType: "alarmTimeConfig"
    title: "แจ้งเตือนเข้างาน"
    timeZone: "Asia/Bangkok"
    isRecurring: true
    recurrence: { type: INTERVAL, interval: HOURS, value: 24 }
    firstNotificationAt: "2026-11-01T02:00:00Z"
    expiryAt: "2027-11-01T02:00:00Z"
  }) {
    _id scheduleKey nextNotificationAt
  }
}
```

Input `CreateScheduleInput`

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | String | ไม่ใส่ = แอปที่ login |
| serviceKey | String! | service ที่จะได้รับ alarm |
| scheduleRefKey | String | key อ้างอิงข้อมูลฝั่ง service เจ้าของ |
| scheduleRefType | String | ประเภทข้อมูลฝั่ง service เจ้าของ |
| title | String | ชื่อ |
| description | String | รายละเอียด |
| isActive | Boolean | เปิดใช้งาน |
| timeZone | String | เขตเวลา เช่น `Asia/Bangkok` |
| isRecurring | Boolean | วนซ้ำหรือไม่ (ค่าเริ่มต้น `false`) |
| recurrence | CreateScheduleRecurrenceInput | `type` (ค่าเริ่มต้น `INTERVAL`), `interval`, `value`, `specific { dayOfWeek, dayOfMonth }` |
| firstNotificationAt | Date! | เวลาแจ้งเตือนครั้งแรก |
| expiryAt | Date | เวลาหมดอายุ |

Response : `Schedule`

---

#### Update Schedule

แก้ไข schedule · permission: `updateSchedule`

```graphql
mutation {
  updateSchedule(
    scheduleId: "6650f0c2a1b2c3d4e5f60718"
    input: { title: "แจ้งเตือนเข้างาน (ใหม่)", isRecurring: true, isActive: true }
  ) {
    _id title isActive
  }
}
```

Input: `scheduleId: String!`, `appKey: String` (ไม่ใส่ = แอปที่ login), `input: UpdateScheduleInput` (ฟิลด์เดียวกับ CreateScheduleInput ยกเว้น appKey และไม่บังคับทุกฟิลด์) · ควรส่ง `isRecurring` ทุกครั้งที่แก้ schedule แบบวนซ้ำ (ค่าเริ่มต้นของฟิลด์นี้คือ `false`)

Response : `Schedule`

---

#### Delete Schedule

ลบ schedule · permission: `deleteSchedule`

```graphql
mutation {
  deleteSchedule(scheduleId: "6650f0c2a1b2c3d4e5f60718") { _id title }
}
```

Input: `scheduleId: ID!`, `appKey: String`

Response : `Schedule` (ข้อมูลที่ถูกลบ)

---

#### Sent Schedule

สั่งส่ง alarm ของ schedule นี้ทันทีโดยไม่รอเวลา (ส่ง `schedule-alarm` ไปยัง service เจ้าของ)

```graphql
mutation {
  sentSchedule(scheduleId: "6650f0c2a1b2c3d4e5f60718") { _id notificationCount lastSentAt }
}
```

Input: `scheduleId: ID!`, `appKey: String`

Response : `Schedule`

---

## kafka consume Reference

ทุก topic ตรวจว่า schedule มีสิทธิ์ในแอปตาม header `appKey` (มี appCertificate ของแอปนั้น) ก่อนทำงาน

### Standard topics

topic มาตรฐานของ service ใน core set

| topic | คำอธิบาย |
| --- | --- |
| `init-system` | ตั้งระบบจากศูนย์ (เฉพาะ core set) |
| `refresh-data` | ส่งข้อมูลที่ตัวเองถือขึ้นไปใหม่ (เช่น permission) และล้าง cache |
| `sync-app-certificate` | รับ appCertificate ของแอปที่ schedule มีสิทธิ์ |
| `sync-app-credential` | รับข้อมูลการเข้าใช้ของ user ในแอป (ใช้ตรวจ token) |
| `sync-application` | รับข้อมูลแอป |
| `sync-user-policy` | รับ UserPolicy สำหรับตรวจ permission |

---

### Set Schedule

ลงทะเบียน / ยกเลิกนัดหมายจาก service อื่น · นี่คือช่องทางหลักที่ service อื่นควรใช้

    topic: set-schedule
    header: appKey = แอปของนัดหมาย, serviceKey = schedule

#### Add

สร้าง schedule ใหม่ (ทุกข้อความ ADD สร้างรายการใหม่เสมอ ⇒ อย่าส่งซ้ำสำหรับนัดเดียวกัน)

    Action: ADD

```json
{
  "action": "ADD",
  "schedule": {
    "appKey": "yourAppKey",
    "serviceKey": "yourServiceKey",
    "scheduleRefKey": "665000000000000000000001",
    "scheduleRefType": "alarmTimeConfig",
    "alarmData": { "timeRecordId": "abcd", "timeConfigType": "startTime" },
    "title": "แจ้งเตือนเข้างาน",
    "description": "แจ้งเตือนเข้างาน",
    "timeZone": "Asia/Bangkok",
    "isRecurring": true,
    "recurrence": { "type": "INTERVAL", "interval": "HOURS", "value": 24 },
    "notificationAt": "2026-11-01T02:00:00Z",
    "expiryAt": "2027-11-01T02:00:00Z"
  }
}
```

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | string | แอปของนัดหมาย |
| serviceKey | string | service ที่จะได้รับ `schedule-alarm` (ปกติคือตัวผู้ส่งเอง) |
| scheduleRefKey | string | key อ้างอิงข้อมูลฝั่งผู้ส่ง (ใช้ตอน REMOVE ด้วย) |
| scheduleRefType | string | ประเภทข้อมูลฝั่งผู้ส่ง ใช้แยกว่า alarm นี้เป็นงานแบบไหน |
| alarmData | object | ข้อมูลอิสระ จะได้คืนใน `schedule-alarm` |
| title, description | string | ชื่อ / รายละเอียด |
| timeZone | string | ใช้คำนวณ "วันในสัปดาห์ / วันที่ของเดือน" ของนัดซ้ำแบบ `SPECIFIC` ตามเวลาท้องถิ่น · ไม่ใส่ = เขตเวลาของเครื่อง schedule ⇒ ควรระบุเสมอ |
| isRecurring | boolean | วนซ้ำหรือไม่ |
| recurrence | object | `{ type, interval, value, specific: { dayOfWeek, dayOfMonth } }` ดู [ScheduleRecurrence](#get-schedules) · ไม่วนซ้ำให้ส่ง `null` |
| notificationAt | Date | **เวลาที่ยิงจริง** (ครั้งแรก) เป็นเวลาสากล ISO 8601 เช่น `2026-11-01T02:00:00Z` = 09:00 เวลาไทย — `timeZone` ไม่เปลี่ยนค่านี้ |
| expiryAt | Date | เวลาหมดอายุ (ไม่ใส่ = notificationAt + 1 ปี) |

#### Remove

ยกเลิก schedule ทั้งหมดของแอปที่ตรงกับ `scheduleRefKey` + `scheduleRefType`

    Action: REMOVE

| key | Type | คำอธิบาย |
| --- | --- | --- |
| schedule.appKey | string | แอปของนัดหมาย |
| schedule.scheduleRefKey | string | key อ้างอิงที่ใช้ตอน ADD |
| schedule.scheduleRefType | string | ประเภทที่ใช้ตอน ADD |

---

### Schedule Alarm Result

service ปลายทางตอบกลับหลังได้รับ `schedule-alarm` เพื่อบันทึกประวัติ · schedule จับคู่ด้วย `appKey` + `alarmRefKey` แล้วตั้ง `isSuccess = true` และ `scheduleReceivedAt`

    topic: schedule-alarm-result
    header: appKey, serviceKey = schedule
    Action: ADD

| key | Type | คำอธิบาย |
| --- | --- | --- |
| scheduleAlarm.id | string | id ของ schedule (ส่งคืนตามที่ได้รับ) |
| scheduleAlarm.appKey | string | appKey |
| scheduleAlarm.serviceKey | string | serviceKey ของผู้ตอบ |
| scheduleAlarm.scheduleRefKey | string | ตามที่ได้รับ |
| scheduleAlarm.scheduleRefType | string | ตามที่ได้รับ |
| scheduleAlarm.alarmData | object | ตามที่ได้รับ |
| scheduleAlarm.alarmRefKey | string | **ต้องส่งคืน** ใช้จับคู่ประวัติ |
| scheduleAlarm.clientReceivedAt | Date | เวลาที่ผู้ตอบได้รับ alarm |
| scheduleAlarm.clientSentAt | Date | เวลาที่ผู้ตอบส่งผลกลับ |

---

## Kafka Produce Reference

### Schedule Alarm

ส่งเมื่อถึงเวลาของ schedule (หรือสั่งด้วย `sentSchedule`) · เป็น topic เดียวที่ทุก service subscribe ได้ ผู้รับต้องกรองเอาเฉพาะของตัวเองด้วย `scheduleAlarm.serviceKey` (หรือ header `serviceKey`) และแยกงานด้วย `scheduleRefType`

    topic: schedule-alarm
    header: appKey = แอปของนัดหมาย, serviceKey = service เจ้าของนัดหมาย
    Action: ADD

```json
{
  "action": "ADD",
  "scheduleAlarm": {
    "id": "6650f0c2a1b2c3d4e5f60718",
    "appKey": "yourAppKey",
    "serviceKey": "yourServiceKey",
    "scheduleRefKey": "665000000000000000000001",
    "scheduleRefType": "alarmTimeConfig",
    "alarmData": { "timeRecordId": "abcd", "timeConfigType": "startTime" },
    "alarmRefKey": "Xy12Ab34Cd"
  }
}
```

| key | Type | คำอธิบาย |
| --- | --- | --- |
| id | string | id ของ schedule |
| appKey | string | appKey |
| serviceKey | string | service ที่ต้องทำงาน |
| scheduleRefKey | string | ตามที่ลงทะเบียน |
| scheduleRefType | string | ตามที่ลงทะเบียน |
| alarmData | object | ตามที่ลงทะเบียน |
| alarmRefKey | string | key ของการส่งรอบนี้ ให้ส่งคืนใน `schedule-alarm-result` |

ผู้รับควร produce `schedule-alarm-result` กลับมาหลังรับงาน เพื่อให้ประวัติการส่งสมบูรณ์

---

### Set Schedule Result

ผลการลงทะเบียนจาก `set-schedule`

    topic: set-schedule-result
    Action: SUCCESS | ERROR

| key | Type | คำอธิบาย |
| --- | --- | --- |
| action | string | `SUCCESS` หรือ `ERROR` |
| schedule | object | ข้อมูล schedule ที่ส่งมา |
| error | object | รายละเอียดข้อผิดพลาด (กรณี ERROR) |

---

### Sync Permission

ส่ง permission ของ schedule ให้ access-control เมื่อได้รับ `refresh-data` เพื่อนำไปผูกกับ role (`getSchedules`, `getScheduleById`, `getScheduleByKey`, `createSchedule`, `updateSchedule`, `deleteSchedule`, `sentSchedule`, `getScheduleTransactions`, `getScheduleTransactionById`, `systemApp`)

    topic: sync-permission
    Action: ADD

---

> อัปเดตจากโค้ด gumon-schedule-service@4bea2c8 · 2026-10-05
