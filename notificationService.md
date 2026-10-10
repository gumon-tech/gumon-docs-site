# Notification Service

Service กลางสำหรับส่งการแจ้งเตือนของทุก service ในระบบ · serviceKey: `notification`

ช่องทางที่รองรับ

- **In-app** แจ้งเตือนในแอปแบบ realtime ผ่าน WebSocket (Socket.IO) และเก็บเป็นรายการให้ดึงย้อนหลัง / อ่าน / ปิดได้
- **Email** ส่งผ่าน SMTP provider ที่ตั้งค่าต่อแอป (เลือกตาม priority และสลับ provider อัตโนมัติเมื่อเกิน rate limit หรือส่งไม่สำเร็จ)
- **SMS** ส่งผ่าน SMS provider ที่ตั้งค่าต่อแอป (หลักการเดียวกับ email)
- **Webhook** มี API จัดการ provider แล้ว ช่องทางส่งอยู่ระหว่างพัฒนา

service อื่นขอส่งแจ้งเตือนผ่าน Kafka topic `create-notification` (ไม่เรียก API ของ notification ตรง) · ตั้งเวลาส่งล่วงหน้าได้ โดย notification จะฝากเวลาไว้กับ [Schedule Service](scheduleService.md) ให้เอง

ทุกช่องทางส่งผ่านคิว (Redis) ทำให้รัน notification หลาย replica ได้ ทั้งฝั่งคิวและ WebSocket

<br>

- [การขอส่งแจ้งเตือนจาก service อื่น](#การขอสงแจงเตอนจาก-service-อน)
- [การเชื่อมต่อ WebSocket](#การเชอมตอ-websocket)
- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [kafka produce Reference](#kafka-produce-reference)

---

<br>
<br>

## การขอส่งแจ้งเตือนจาก service อื่น

produce ข้อความเข้า topic `create-notification` · header `appKey` = แอปที่จะส่ง, `serviceKey` = `notification` (service ปลายทาง)

ใส่ช่องทางที่ต้องการได้หลายช่องทางในข้อความเดียว (`appNotification`, `email`, `sms`) ช่องทางที่ไม่ใส่จะไม่ถูกส่ง

```json
{
  "action": "ADD",
  "notification": {
    "isSchedule": false,
    "organizationId": "665000000000000000000010",
    "refKey": "665000000000000000000020",
    "refType": "leaveRequest",
    "appNotification": {
      "type": "SELECT",
      "authId": ["665000000000000000000030"],
      "notificationType": "GENERAL",
      "title": "คำขอลาได้รับการอนุมัติ",
      "content": "คำขอลาวันที่ 1 พ.ย. ได้รับการอนุมัติแล้ว",
      "from": "SYSTEM",
      "to": "665000000000000000000030",
      "displayType": "SUCCESS",
      "url": "/leave/665000000000000000000020"
    },
    "email": {
      "from": "noreply@example.com",
      "to": "user@example.com",
      "subject": "คำขอลาได้รับการอนุมัติ",
      "html": "<p>คำขอลาของคุณได้รับการอนุมัติแล้ว</p>"
    }
  }
}
```

ตั้งเวลาส่ง: ใส่ `"isSchedule": true` และ `schedule` (โครงเดียวกับ payload `set-schedule` ของ [Schedule Service](scheduleService.md#set-schedule) เช่น `notificationAt`, `timeZone`, `isRecurring`, `recurrence`, `expiryAt`) · notification จะสร้างรายการไว้ก่อนแล้วลงทะเบียนเวลากับ schedule เอง ผู้ขอไม่ต้องส่ง `set-schedule` เอง

**ยกเลิกแจ้งเตือนที่ตั้งเวลาไว้** (เช่น นัดถูกยกเลิก ไม่ต้องส่ง SMS เตือนแล้ว) — ส่ง `REMOVE` พร้อม `refKey` + `refType` เดียวกับที่ใช้ตอน `ADD`

```json
{ "action": "REMOVE", "notification": { "refKey": "665000000000000000000020", "refType": "booking.reminder" } }
```

- ยกเลิกทุกรายการที่ยังไม่ส่ง (in-app / email / SMS) ของ `refKey` + `refType` นั้นในแอปเดียวกัน และแจ้ง schedule ให้ถอนเวลา ⇒ ไม่ถูกส่ง
- ต้องใส่ทั้ง `refKey` และ `refType` (ไม่ครบ = ERROR ไม่ยกเลิกอะไร) · ส่งซ้ำ หรืออ้างถึงที่ไม่มี = SUCCESS โดย `removedCount: 0`
- จะยกเลิกได้ ตอน `ADD` ต้องใส่ `refKey` / `refType` ไว้ · ตั้ง `refType` ขึ้นต้นด้วยชื่อ service ของตัวเอง (เช่น `booking.reminder`) กันชนกับ service อื่นในแอปเดียวกัน

ผลการรับคำขอส่งกลับทาง topic `create-notification-result` (`SUCCESS` / `ERROR`)

รายละเอียดฟิลด์ดูที่ [Create Notification](#create-notification)

---

## การเชื่อมต่อ WebSocket

ใช้รับแจ้งเตือน in-app แบบ realtime ที่หน้าบ้าน

1. เรียก [Get My AppNotification Config](#get-my-appnotification-config) เพื่อรับ `roomId` ของ user ที่ login
2. เชื่อมต่อ Socket.IO (default namespace) ที่ host ของ notification service โดยส่ง header `authorization` (access token) และ `roomId` (ทาง query `?roomId=` หรือ header `roomid`)
3. ฟัง event `appNotification` ได้ข้อมูลรูป `{ action, data }`
   - `action: CREATE` = แจ้งเตือนใหม่
   - `action: UPDATE` = แจ้งเตือนเดิมถูกอ่าน / ปิด
   - `data` = ข้อมูล [AppNotification](#appnotification)

```js
import { io } from "socket.io-client";

const socket = io(NOTIFICATION_URL, {
  query: { roomId },
  extraHeaders: { authorization: `Bearer ${accessToken}` },
});

socket.on("appNotification", ({ action, data }) => {
  console.log(action, data.content.title);
});
```

---

## API Reference
---

GraphQL endpoint `/graphql` · ทุก API ต้อง login (header `authorization`) และต้องมี **permission ชื่อเดียวกับ API** (เช่น `getAppNotifications`) ยกเว้น API แบบ `...ByAppKey` ที่ใช้ app credential ของระบบภายนอก (header `X-APP-CLIENT-ID` + `X-APP-CLIENT-SECRET`) · การระบุ `appKey` อื่นนอกจากแอปที่ login ต้องมี permission `systemApp`

API ที่มีคำว่า `My` ทำงานกับข้อมูลของ user ที่ login เท่านั้น ส่วน API ที่ไม่มี `My` ทำงานกับข้อมูลทั้งแอป (สำหรับผู้ดูแล)

ทุก API แบบรายการรับ input รูปเดียวกัน `{ filter, search, sort: { sortBy, sortOrder }, pagination: { limit, page } }` และคืน `pagination { limit page totalItems totalPages hasPrevPage hasNextPage prevPage nextPage }`

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

#### Get My AppNotification Config

คืน `roomId` (String) ของ user ที่ login สำหรับเชื่อมต่อ WebSocket

```graphql
query {
  getMyAppNotificationConfig
}
```

---

#### Get App Notifications

ดึงแจ้งเตือน in-app ทั้งหมดของแอป

```graphql
query {
  getAppNotifications(input: {
    filter: { isRead: false, notificationType: URGENT }
    sort: { sortBy: createdAt, sortOrder: DESC }
    pagination: { limit: 20, page: 1 }
  }) {
    appNotifications { _id authId notificationType isRead isClosed sentAt content { title content displayType url } }
    pagination { totalItems totalPages }
  }
}
```

filter: `organizationId`, `authId`, `isSchedule`, `scheduleRefKey`, `scheduleRefType`, `sentStatus`, `isReSent`, `reSentBy`, `notificationType`, `isRead`, `isClosed`, `displayType`, `createdBy`, `updatedBy`, `createdAtStart/End`, `updatedAtStart/End` · search: `organizationId`, `scheduleRefKey`, `scheduleRefType`, `content { title content from to }`

Response : `AppNotificationPagination` = `{ appNotifications: [AppNotification], pagination }`

##### AppNotification

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| organizationId | String | องค์กรที่ส่ง (ถ้าส่งในนามองค์กร) |
| authId | String | user ผู้รับ |
| isSchedule | Boolean | เป็นการตั้งเวลาส่งหรือไม่ |
| scheduleRefKey / scheduleRefType | String | อ้างอิงกับ schedule (เมื่อ isSchedule = true) |
| sentStatus | ENUM | `PREPARE` (รอเข้าคิว) · `QUEUED` · `SUCCEED` · `FAIL` |
| sentAt | Date | เวลาที่จะส่ง / ส่ง |
| succeedSentAt | Date | เวลาที่ส่งสำเร็จ |
| isReSent / reSentBy | Boolean / String | ถูกส่งซ้ำแล้ว / id ต้นฉบับ |
| notificationType | ENUM | `VERY_URGENT`, `URGENT`, `IMPORTANT`, `GENERAL`, `REMINDER`, `APP_NOTIFICATION` |
| isRead / readAt | Boolean / Date | อ่านแล้ว / เวลาอ่าน |
| isClosed / closedAt | Boolean / Date | ปิดการแสดงแล้ว (ยังดึงย้อนหลังได้) / เวลาปิด |
| content.title | String | หัวข้อ |
| content.content | String | เนื้อหา |
| content.from / content.to | String | ข้อความบอกผู้ส่ง / ผู้รับ เช่น "จากโรงแรม A", "ถึงคุณสมชาย" |
| content.displayType | ENUM | `SUCCESS`, `INFO`, `WARNING`, `ERROR` (ให้หน้าบ้านเลือกรูปแบบแสดงผล) |
| content.url | String | ลิงก์เมื่อกดแจ้งเตือน |
| createdAt / updatedAt | Date | |

---

#### Get App Notification By ID

```graphql
query {
  getAppNotificationById(appNotificationId: "6650f0c2a1b2c3d4e5f60718") { _id authId isRead content { title } }
}
```

Response : `AppNotification`

---

#### Get My App Notification

ดึงแจ้งเตือน in-app ของ user ที่ login (filter เหมือน Get App Notifications ยกเว้น `authId`)

```graphql
query {
  getMyAppNotification(input: { filter: { isClosed: false }, sort: { sortBy: createdAt, sortOrder: DESC }, pagination: { limit: 20, page: 1 } }) {
    appNotifications { _id isRead content { title content displayType url } createdAt }
    pagination { totalItems hasNextPage }
  }
}
```

Response : `AppNotificationPagination`

---

#### Get My App Notification By ID

```graphql
query {
  getMyAppNotificationById(appNotificationId: "6650f0c2a1b2c3d4e5f60718") { _id isRead content { title content } }
}
```

Response : `AppNotification`

---

#### Get Email Transactions

ดึงประวัติ email ทั้งหมดของแอป

```graphql
query {
  getEmailTransactions(input: { filter: { sentStatus: FAIL }, pagination: { limit: 20, page: 1 } }) {
    emailTransactions { _id sentStatus sentAt succeedSentAt content { to subject } transactionLogs { sentAt isSucceed } }
    pagination { totalItems }
  }
}
```

filter: `organizationId`, `isSchedule`, `scheduleRefKey`, `scheduleRefType`, `sentStatus`, `isReSent`, `reSentBy`, `createdBy`, `updatedBy` · search: `scheduleRefKey`, `scheduleRefType`

Response : `EmailTransactionPagination` = `{ emailTransactions: [EmailTransaction], pagination }`

##### EmailTransaction

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| organizationId | String | องค์กรที่ส่ง |
| isSchedule, scheduleRefKey, scheduleRefType | | การตั้งเวลา (เหมือน AppNotification) |
| sentStatus | ENUM | `PREPARE`, `QUEUED`, `SUCCEED`, `FAIL` |
| sentAt / succeedSentAt | Date | เวลาที่จะส่ง / เวลาส่งสำเร็จ |
| isReSent / reSentBy | Boolean / String | การส่งซ้ำ |
| content | EmailTransactionContent | `to`, `subject`, `html`, `text`, `attachments [JSON]`, `cc`, `bcc`, `from` |
| transactionLogs | [EmailTransactionLog] | ประวัติแต่ละครั้งที่พยายามส่ง: `sentAt`, `emailProviderId`, `isSucceed`, `response` |

---

#### Get My Email Transactions

ดึงประวัติ email ที่ user ที่ login เป็นผู้สร้าง · input / response เหมือน Get Email Transactions

```graphql
query {
  getMyEmailTransactions(input: { pagination: { limit: 20, page: 1 } }) { emailTransactions { _id sentStatus content { to subject } } }
}
```

---

#### Get Email Transaction By ID

```graphql
query {
  getEmailTransactionById(emailTransactionId: "6650f0c2a1b2c3d4e5f60718") { _id sentStatus content { to subject } transactionLogs { isSucceed response } }
}
```

Input: `emailTransactionId: ID!`, `appKey: String` · Response : `EmailTransaction`

---

#### Get My Email Transaction By ID

```graphql
query {
  getMyEmailTransactionById(emailTransactionId: "6650f0c2a1b2c3d4e5f60718") { _id sentStatus }
}
```

Response : `EmailTransaction`

---

#### Get Email Transactions By AppKey

สำหรับระบบภายนอกที่เรียกด้วย app credential

```graphql
query {
  getEmailTransactionsByAppKey(appKey: "yourAppKey", input: { pagination: { limit: 20, page: 1 } }) { emailTransactions { _id sentStatus } }
}
```

Response : `EmailTransactionPagination`

---

#### Get Sms Transactions

ดึงประวัติ SMS ทั้งหมดของแอป · input เหมือน Get Email Transactions

```graphql
query {
  getSmsTransactions(input: { filter: { sentStatus: SUCCEED }, pagination: { limit: 20, page: 1 } }) {
    smsTransactions { _id sentStatus sentAt content { msisdn message sender } }
    pagination { totalItems }
  }
}
```

Response : `SmsTransactionPagination` = `{ smsTransactions: [SmsTransaction], pagination }`

SmsTransaction มีฟิลด์สถานะ / การตั้งเวลา / การส่งซ้ำ / `transactionLogs` เหมือน EmailTransaction และ `content`:

| key | Type | คำอธิบาย |
| --- | --- | --- |
| msisdn | String | เบอร์ผู้รับ |
| message | String | ข้อความ |
| sender | String | ชื่อผู้ส่ง (sender name) |
| scheduled_delivery | String | เวลาส่งฝั่ง provider (ถ้า provider รองรับ) |
| force | String | ตัวเลือกเฉพาะ provider |
| shorten_url / tracking_url | String | ตัวเลือกย่อ / ติดตามลิงก์ (ถ้า provider รองรับ) |
| expire | String | อายุข้อความ (ถ้า provider รองรับ) |

---

#### Get My Sms Transactions

```graphql
query {
  getMySmsTransactions(input: { pagination: { limit: 20, page: 1 } }) { smsTransactions { _id sentStatus content { msisdn } } }
}
```

Response : `SmsTransactionPagination`

---

#### Get Sms Transaction By ID

```graphql
query {
  getSmsTransactionById(smsTransactionId: "6650f0c2a1b2c3d4e5f60718") { _id sentStatus content { msisdn message } }
}
```

Input: `smsTransactionId: ID!`, `appKey: String` · Response : `SmsTransaction`

---

#### Get My Sms Transaction By ID

```graphql
query {
  getMySmsTransactionById(smsTransactionId: "6650f0c2a1b2c3d4e5f60718") { _id sentStatus }
}
```

Response : `SmsTransaction`

---

#### Get Sms Transactions By AppKey

สำหรับระบบภายนอกที่เรียกด้วย app credential

```graphql
query {
  getSmsTransactionsByAppKey(appKey: "yourAppKey", input: { pagination: { limit: 20, page: 1 } }) { smsTransactions { _id sentStatus } }
}
```

Response : `SmsTransactionPagination`

---

#### Get Email Providers

ดึงรายการ email provider (SMTP) ของแอป

```graphql
query {
  getEmailProviders(input: { pagination: { limit: 10, page: 1 } }) {
    emailProviders { _id emailProviderKey title priority isActive maxEmailsPerMinute maxRetriesPerProvider totalSentCount totalFailedCount config { senderName host port secure } }
    pagination { totalItems }
  }
}
```

##### EmailProvider

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| emailProviderKey | String | key อ้างอิง |
| priority | Int | ลำดับการเลือกใช้ |
| title / description | String | ชื่อ / รายละเอียด |
| isActive | Boolean | เปิดใช้งาน |
| config | EmailProviderConfig | `senderName`, `host`, `port`, `secure`, `username` |
| maxEmailsPerMinute | Int | จำนวน email สูงสุดต่อนาที ก่อนสลับไป provider ถัดไป |
| maxRetriesPerProvider | Int | จำนวนครั้งที่ลองส่งซ้ำก่อนสลับ provider |
| totalSentCount / totalFailedCount | Int | สถิติการส่ง |
| sentDueToLimitCount, failedDueToLimitCount, sentDueToMaxRetriesCount, failedDueToMaxRetriesCount | Int | สถิติการสลับ provider |

---

#### Get Email Provider By ID

```graphql
query {
  getEmailProviderById(emailProviderId: "6650f0c2a1b2c3d4e5f60718") { _id title isActive }
}
```

Response : `EmailProvider`

---

#### Get Email Provider By Key

```graphql
query {
  getEmailProviderByKey(emailProviderKey: "main-smtp") { _id title isActive }
}
```

Response : `EmailProvider`

---

#### Get Email Providers By AppKey

สำหรับระบบภายนอกที่เรียกด้วย app credential

```graphql
query {
  getEmailProvidersByAppKey(appKey: "yourAppKey") { emailProviders { _id title } }
}
```

---

#### Get Sms Providers

ดึงรายการ SMS provider ของแอป · ฟิลด์เหมือน EmailProvider โดยใช้ `smsProviderKey`, `maxSmsPerMinute` และเพิ่ม `providerType`

```graphql
query {
  getSmsProviders(input: { pagination: { limit: 10, page: 1 } }) {
    smsProviders { _id smsProviderKey title providerType priority isActive maxSmsPerMinute }
  }
}
```

`providerType`: enum `THAIBULKSMS`, `SMSMKT`, `INFOBIP`, `THSMS`, `MAILBIT`, `AWS`, `TWILIO` · ที่ส่งได้ในปัจจุบันคือ `THAIBULKSMS` และ `TWILIO`

---

#### Get Sms Provider By ID

```graphql
query {
  getSmsProviderById(smsProviderId: "6650f0c2a1b2c3d4e5f60718") { _id title providerType }
}
```

---

#### Get Sms Provider By Key

```graphql
query {
  getSmsProviderByKey(smsProviderKey: "main-sms") { _id title providerType }
}
```

---

#### Get Sms Providers By AppKey

สำหรับระบบภายนอกที่เรียกด้วย app credential

```graphql
query {
  getSmsProvidersByAppKey(appKey: "yourAppKey") { smsProviders { _id title } }
}
```

---

#### Get Webhook Providers

ดึงรายการ webhook provider ของแอป · ฟิลด์เหมือน EmailProvider โดยใช้ `webhookProviderKey`, `maxWebhooksPerMinute` (ไม่มี priority)

```graphql
query {
  getWebhookProviders(input: { pagination: { limit: 10, page: 1 } }) { webhookProviders { _id webhookProviderKey title isActive } }
}
```

---

#### Get Webhook Provider By ID

```graphql
query {
  getWebhookProviderById(webhookProviderId: "6650f0c2a1b2c3d4e5f60718") { _id title }
}
```

---

#### Get Webhook Provider By Key

```graphql
query {
  getWebhookProviderByKey(webhookProviderKey: "main-hook") { _id title }
}
```

---

#### Get Webhook Providers By AppKey

สำหรับระบบภายนอกที่เรียกด้วย app credential

```graphql
query {
  getWebhookProvidersByAppKey(appKey: "yourAppKey") { webhookProviders { _id title } }
}
```

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

#### Create App Notification

สร้างแจ้งเตือน in-app จากหน้าบ้าน (ผลเหมือนส่ง `create-notification` ที่มี `appNotification`)

```graphql
mutation {
  createAppNotification(input: {
    type: SELECT
    authIds: ["665000000000000000000030"]
    notificationType: GENERAL
    content: { title: "ประกาศ", content: "ระบบจะปิดปรับปรุงคืนนี้", displayType: WARNING }
    isSchedule: true
    schedule: { notificationAt: "2026-11-01T11:00:00Z", timeZone: "Asia/Bangkok" }
  }) {
    _id authId sentStatus sentAt
  }
}
```

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | String | ไม่ใส่ = แอปที่ login |
| organizationId | String | ส่งในนามองค์กร |
| type | ENUM | `SELECT` (ส่งตาม authIds) · `ALL` (ส่งทุก user ในแอป) ค่าเริ่มต้น `SELECT` |
| authIds | [ID] | ผู้รับ (เมื่อ type = SELECT) |
| content | CreateAppNotificationContentInput! | `title`, `content`, `from`, `to`, `displayType` (ค่าเริ่มต้น `INFO`), `url` |
| notificationType | ENUM | ค่าเริ่มต้น `APP_NOTIFICATION` |
| isSchedule | Boolean | ตั้งเวลาส่ง (ค่าเริ่มต้น `false`) |
| schedule | CreateAppNotificationScheduleInput | `notificationAt`, `timeZone`, `isRecurring`, `recurrence { type interval value specific { dayOfWeek dayOfMonth } }`, `expiryAt` |

Response : `[AppNotification]` (หนึ่งรายการต่อผู้รับ)

---

#### Read App Notification

ตั้งแจ้งเตือนของแอปเป็นอ่านแล้ว (ของ user ใดก็ได้ในแอป สำหรับผู้ดูแล)

```graphql
mutation {
  readAppNotification(input: { type: SELECT, appNotificationIds: ["6650f0c2a1b2c3d4e5f60718"] }) { _id isRead readAt }
}
```

Input `ReadAppNotificationInput`: `type` `SELECT` (เฉพาะ `appNotificationIds`) / `ALL` · `appNotificationIds: [ID]`

Response : `[AppNotification]`

---

#### Read My App Notification

ตั้งแจ้งเตือนของ user ที่ login เป็นอ่านแล้ว · `type: ALL` = อ่านทั้งหมด

```graphql
mutation {
  readMyAppNotification(input: { type: ALL }) { _id isRead }
}
```

---

#### Closed App Notification

ปิดการแสดงแจ้งเตือนของแอป (สำหรับผู้ดูแล) · input `CloseAppNotificationInput` รูปเดียวกับ Read

```graphql
mutation {
  closedAppNotification(input: { type: SELECT, appNotificationIds: ["6650f0c2a1b2c3d4e5f60718"] }) { _id isClosed closedAt }
}
```

---

#### Closed My App Notification

ปิดการแสดงแจ้งเตือนของ user ที่ login

```graphql
mutation {
  closedMyAppNotification(input: { type: SELECT, appNotificationIds: ["6650f0c2a1b2c3d4e5f60718"] }) { _id isClosed }
}
```

---

#### Create Email Transaction

สร้างและส่ง email จากหน้าบ้าน

```graphql
mutation {
  createEmailTransaction(input: {
    content: { from: "noreply@example.com", to: "user@example.com", subject: "ยินดีต้อนรับ", html: "<p>สวัสดี</p>" }
  }) {
    _id sentStatus
  }
}
```

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | String | ไม่ใส่ = แอปที่ login |
| organizationId | String | ส่งในนามองค์กร |
| isSchedule | Boolean | ตั้งเวลาส่ง (ค่าเริ่มต้น `false`) |
| sentAt | Date | เวลาที่ต้องการส่ง |
| content.to | String! | ผู้รับ |
| content.subject | String! | หัวข้อ |
| content.from | String! | ผู้ส่ง |
| content.html / content.text | String | เนื้อหา HTML / ข้อความล้วน (ส่งเนื้อหาสำเร็จรูปมาเอง ไม่มี template) |
| content.cc / content.bcc | String | คั่นด้วย comma |
| content.attachments | [JSON] | ไฟล์แนบ (รูปแบบ attachment ของ nodemailer) |

Response : `EmailTransaction`

---

#### Delete Email Transaction

ลบประวัติ email ได้หลายรายการ

```graphql
mutation {
  deleteEmailTransaction(emailTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id }
}
```

Input: `emailTransactionIds: [ID]!`, `appKey: String` · Response : `[EmailTransaction]`

---

#### Delete My Email Transaction

ลบประวัติ email ของ user ที่ login

```graphql
mutation {
  deleteMyEmailTransaction(emailTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id }
}
```

---

#### Resent Email Transaction

ส่งซ้ำ: สร้างรายการใหม่จากรายการเดิม (รายการเดิมถูกตั้ง `isReSent = true`, รายการใหม่มี `reSentBy` = id เดิม)

```graphql
mutation {
  resentEmailTransaction(emailTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id reSentBy sentStatus }
}
```

---

#### Resent My Email Transaction

ส่งซ้ำ email ของ user ที่ login

```graphql
mutation {
  resentMyEmailTransaction(emailTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id reSentBy }
}
```

---

#### Sent All Queue Email Transaction

นำ email ที่ยังค้างสถานะคิวกลับเข้าคิวส่งอีกครั้ง (สำหรับผู้ดูแล)

```graphql
mutation {
  sentAllQueueEmailTransaction { _id sentStatus }
}
```

Input: `appKey: String` · Response : `[EmailTransaction]`

---

#### Create Sms Transaction

สร้างและส่ง SMS จากหน้าบ้าน

```graphql
mutation {
  createSmsTransaction(input: { content: { msisdn: "0812345678", message: "รหัสยืนยันของคุณคือ 123456", sender: "MyApp" } }) {
    _id sentStatus
  }
}
```

Input: `appKey`, `organizationId`, `isSchedule`, `sentAt`, `content` (ฟิลด์ตาม [Get Sms Transactions](#get-sms-transactions)) · Response : `SmsTransaction`

---

#### Delete Sms Transaction

```graphql
mutation {
  deleteSmsTransaction(smsTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id }
}
```

---

#### Delete My Sms Transaction

```graphql
mutation {
  deleteMySmsTransaction(smsTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id }
}
```

---

#### Resent Sms Transaction

```graphql
mutation {
  resentSmsTransaction(smsTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id reSentBy }
}
```

---

#### Resent My Sms Transaction

```graphql
mutation {
  resentMySmsTransaction(smsTransactionIds: ["6650f0c2a1b2c3d4e5f60718"]) { _id reSentBy }
}
```

---

#### Sent All Queue Sms Transaction

นำ SMS ที่ยังค้างสถานะคิวกลับเข้าคิวส่งอีกครั้ง (สำหรับผู้ดูแล)

```graphql
mutation {
  sentAllQueueSmsTransaction { _id sentStatus }
}
```

---

#### Create Email Provider

เพิ่ม SMTP provider ให้แอป

```graphql
mutation {
  createEmailProvider(create: {
    title: "Main SMTP"
    priority: 0
    config: { senderName: "MyApp", host: "smtp.example.com", port: 587, secure: false, username: "smtp-user", password: "********" }
    maxEmailsPerMinute: 600
    maxRetriesPerProvider: 3
  }) {
    _id emailProviderKey
  }
}
```

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | String | ไม่ใส่ = แอปที่ login |
| emailProviderKey | String | key อ้างอิง |
| priority | Int | ลำดับการเลือกใช้ (ค่าเริ่มต้น 0) |
| title | String! | ชื่อ |
| description | String | รายละเอียด |
| isActive | Boolean | ค่าเริ่มต้น `true` |
| config | CreateEmailProviderConfigInput! | `senderName` (บังคับ), `host`, `port`, `secure`, `username`, `password` |
| maxEmailsPerMinute | Int | ค่าเริ่มต้น 600 |
| maxRetriesPerProvider | Int | ค่าเริ่มต้น 3 |

Response : `EmailProvider`

---

#### Update Email Provider

```graphql
mutation {
  updateEmailProvider(emailProviderId: "6650f0c2a1b2c3d4e5f60718", update: { isActive: false }) { _id isActive }
}
```

Input: `emailProviderId: ID!`, `update: UpdateEmailProviderInput!` (ฟิลด์เดียวกับ create ไม่บังคับ), `appKey: String`

---

#### Delete Email Provider

```graphql
mutation {
  deleteEmailProvider(emailProviderId: "6650f0c2a1b2c3d4e5f60718") { _id }
}
```

---

#### Create Sms Provider

เพิ่ม SMS provider ให้แอป

```graphql
mutation {
  createSmsProvider(create: {
    title: "ThaiBulkSMS"
    providerType: THAIBULKSMS
    config: { senderName: "MyApp", apiKey: "********", apiSecret: "********" }
  }) {
    _id smsProviderKey
  }
}
```

ฟิลด์เหมือน Create Email Provider โดยใช้ `smsProviderKey`, `maxSmsPerMinute` และเพิ่ม `providerType` · `config`: `senderName` (บังคับ), `apiKey`, `apiSecret`, `hostname`, `username`, `password`, `port`, `headers`, `smsBody`, `etc` (ใช้ตามที่ provider แต่ละเจ้าต้องการ)

---

#### Update Sms Provider

```graphql
mutation {
  updateSmsProvider(smsProviderId: "6650f0c2a1b2c3d4e5f60718", update: { priority: 1 }) { _id priority }
}
```

---

#### Delete Sms Provider

```graphql
mutation {
  deleteSmsProvider(smsProviderId: "6650f0c2a1b2c3d4e5f60718") { _id }
}
```

---

#### Create Webhook Provider

```graphql
mutation {
  createWebhookProvider(create: {
    title: "Partner hook"
    config: { senderName: "MyApp", url: "https://partner.example.com/hook", method: POST }
  }) {
    _id webhookProviderKey
  }
}
```

`config`: `senderName` (บังคับ), `url` (บังคับ), `method` (`GET` / `POST` ค่าเริ่มต้น `GET`), `apiKey`, `apiSecret`, `username`, `password`, `headers`, `WebhookBody` · และ `maxWebhooksPerMinute`, `maxRetriesPerProvider`

---

#### Update Webhook Provider

```graphql
mutation {
  updateWebhookProvider(webhookProviderId: "6650f0c2a1b2c3d4e5f60718", update: { isActive: false }) { _id isActive }
}
```

---

#### Delete Webhook Provider

```graphql
mutation {
  deleteWebhookProvider(webhookProviderId: "6650f0c2a1b2c3d4e5f60718") { _id }
}
```

---

## kafka consume Reference

ทุก topic ตรวจว่า notification มีสิทธิ์ในแอปตาม `appKey` (มี appCertificate ของแอปนั้น) ก่อนทำงาน

### Standard topics

| topic | คำอธิบาย |
| --- | --- |
| `init-system` | ตั้งระบบจากศูนย์ (เฉพาะ core set) |
| `refresh-data` | ส่งข้อมูลที่ตัวเองถือขึ้นไปใหม่ (เช่น permission) และล้าง cache |
| `sync-app-certificate` | รับ appCertificate ของแอปที่ notification มีสิทธิ์ |
| `sync-app-credential` | รับข้อมูลการเข้าใช้ของ user ในแอป (ใช้ตรวจ token) |
| `sync-application` | รับข้อมูลแอป |
| `sync-user-policy` | รับ UserPolicy สำหรับตรวจ permission |

---

### Create Notification

คำขอส่งแจ้งเตือนจาก service อื่น · ดูภาพรวมที่ [การขอส่งแจ้งเตือนจาก service อื่น](#การขอสงแจงเตอนจาก-service-อน)

    topic: create-notification
    header: appKey, serviceKey = notification
    Action: ADD | REMOVE

notification

| key | Type | คำอธิบาย |
| --- | --- | --- |
| isSchedule | boolean | `true` = ตั้งเวลาส่งตาม `schedule` · `false` = ส่งทันที |
| schedule | object | โครงเดียวกับ `set-schedule` (`notificationAt`, `timeZone`, `isRecurring`, `recurrence`, `expiryAt`, `title`, `description`) |
| organizationId | string | ส่งในนามองค์กร |
| refKey / refType | string | อ้างอิงข้อมูลฝั่งผู้ขอ · **ใช้ยกเลิกด้วย `REMOVE`** — ตั้ง `refType` ขึ้นต้นด้วยชื่อ service ของตัวเอง |
| appNotification | object | แจ้งเตือน in-app (ดูด้านล่าง) |
| email | object | `to`, `subject`, `html`, `text`, `cc`, `bcc`, `attachments`, `from` |
| sms | object | `msisdn`, `message`, `sender`, `scheduledDelivery`, `force`, `shortenUrl`, `trackingUrl`, `expire` |

appNotification

| key | Type | คำอธิบาย |
| --- | --- | --- |
| type | ENUM | `SELECT` = ส่งตาม `authId` · `ALL` = ส่งทุก user ในแอป |
| authId | string[] | ผู้รับ (เมื่อ type = SELECT) |
| notificationType | ENUM | `VERY_URGENT`, `URGENT`, `IMPORTANT`, `GENERAL`, `REMINDER`, `APP_NOTIFICATION` |
| title | string | หัวข้อ |
| content | string | เนื้อหา |
| from / to | string | ข้อความบอกผู้ส่ง / ผู้รับ |
| displayType | ENUM | `SUCCESS`, `INFO`, `WARNING`, `ERROR` |
| url | string | ลิงก์เมื่อกดแจ้งเตือน |

---

### Schedule Alarm

รับ alarm จาก [Schedule Service](scheduleService.md#schedule-alarm) สำหรับแจ้งเตือนที่ตั้งเวลาไว้ · รับเฉพาะรายการที่ `scheduleAlarm.serviceKey = notification` แล้วแยกช่องทางตาม `scheduleRefType` (`appNotificationTransaction`, `emailTransaction`, `smsTransaction`) จากนั้นนำรายการเข้าคิวส่ง และตอบ `schedule-alarm-result`

    topic: schedule-alarm
    Action: ADD

---

### Sync Profile

รับรายชื่อ user ของแต่ละแอป ใช้สำหรับส่งแจ้งเตือนแบบ `type: ALL`

    topic: sync-profile
    Action: ADD | REMOVE

| key | Type | คำอธิบาย |
| --- | --- | --- |
| profile.id | string | id ของ profile |
| profile.appKey | string | appKey |
| profile.authId | string | id ของ user |
| profile.username, firstName, middleName, lastName, displayName, gender, profileImage | string | ข้อมูล profile |

---

## Kafka Produce Reference

### Set Schedule

ลงทะเบียนเวลาส่งกับ schedule เมื่อคำขอมี `isSchedule = true` (หนึ่งรายการต่อ transaction) · `scheduleRefType` = `appNotificationTransaction` / `emailTransaction` / `smsTransaction`

    topic: set-schedule
    header: appKey, serviceKey = schedule
    Action: ADD

---

### Schedule Alarm Result

ตอบกลับ schedule หลังรับ alarm (ส่งคืน `alarmRefKey` พร้อม `clientReceivedAt`, `clientSentAt`)

    topic: schedule-alarm-result
    header: appKey, serviceKey = schedule
    Action: ADD

---

### Create Notification Result

ผลการรับคำขอจาก `create-notification`

    topic: create-notification-result
    Action: SUCCESS | ERROR

| key | Type | คำอธิบาย |
| --- | --- | --- |
| action | string | `SUCCESS` หรือ `ERROR` |
| requestAction | string | `REMOVE` เมื่อเป็นผลของคำขอยกเลิก |
| notification | object | คำขอเดิม |
| removedCount | number | จำนวนรายการที่ถูกยกเลิก (เฉพาะ `REMOVE`) |
| error | object | รายละเอียดข้อผิดพลาด (กรณี ERROR) |

---

### Sync Permission

ส่ง permission ของ notification ให้ access-control เมื่อได้รับ `refresh-data` เพื่อนำไปผูกกับ role (ชื่อ permission ตรงกับชื่อ API ด้านบน และ `systemApp`)

    topic: sync-permission
    Action: ADD

---

> อัปเดตจากโค้ด gumon-notification-service@318d224 · 2026-10-05
