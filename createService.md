# การสร้าง Service

ขั้นตอนสร้าง service ใหม่จาก `gumon-backend-template` (NestJS) ตั้งแต่ clone จนเสียบเข้าแอปและใช้งานได้

> service จะเขียนด้วยภาษา/framework อื่นก็ได้ ขอแค่คุยผ่าน Kafka ตาม [สิ่งที่ Service ต้องมี](serviceX.md) — template แค่ทำให้เริ่มเร็ว

- [ขั้นตอน](#ขนตอน)
- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)
- [Checklist](#checklist)

---

## ขั้นตอน

### 1. เริ่มจาก template

clone repo `gumon-backend-template` (branch `master`) แล้วตั้งชื่อ service

- `src/constants/serviceKey.ts` → `export const SERVICE_KEY: string = 'my-service';` (kebab-case · ตรงกับที่จะลงทะเบียนกับ core)
  ค่าเริ่มต้น `CHANGE_ME` ทำให้ service **บูตไม่ขึ้นโดยตั้งใจ** — กันลืมตั้ง
- `package.json` → `"name": "gumon-my-service-service"`

stack: NestJS + GraphQL (schema-first) + MongoDB (Mongoose) + Redis + Kafka (kafkajs)

โครงโฟลเดอร์หลัก

| โฟลเดอร์ | ใช้ทำอะไร |
| --- | --- |
| `src/graphqls/<module>/` | `*.graphql`, resolver, service, dto ของ API |
| `src/constants/` | `serviceKey.ts`, `kafka/kafkaTopic.ts`, `permissionKey.ts`, `permissions.ts`, `redisKey.ts` |
| `src/database/schemas/` | schema ของ MongoDB (collection ขึ้นต้นด้วย `<serviceKey>_`) |
| `src/kafka/` | producer / consumer wrapper |
| `src/kafka-consumer/<topic>/` | 1 โฟลเดอร์ต่อ 1 topic ที่รับ |
| `src/guards/` | ยืนยันตัวตน + ตรวจ UserPolicy |
| `scripts/` | `dev-keys.ts` · `gumon-register.ts` |
| `test/` | e2e + `docker-compose.e2e.yml` |

AI ที่ช่วยเขียนโค้ดอ่านกติกาจาก `AGENTS.md` และ `.github/skills/` ใน repo ได้ทันที

### 2. รันในเครื่อง (ยังไม่ต้องมี core)

template มี Kafka + MongoDB (replica set) + Redis ใน Docker และกุญแจสำหรับใช้ในเครื่องให้พร้อม

```bash
pnpm install
pnpm generate          # สร้าง typings จาก *.graphql
pnpm test:e2e:up       # เปิด Kafka (localhost:19092) · MongoDB (localhost:27099) · Redis (localhost:6399)
pnpm dev:keys          # สร้างกุญแจ .gumon สำหรับใช้ในเครื่อง
pnpm start:dev
```

`.env` สำหรับรันในเครื่อง (ตั้งต้นจาก `.env.example`)

```
DB_URI='mongodb://localhost:27099/dev?directConnection=true'
KAFKA_BROKERS='localhost:19092'
REDIS_HOST='localhost'
REDIS_PORT='6399'
GRAPHQL_PLAYGROUND='true'
```

- กุญแจจาก `dev:keys` มีรูปแบบเดียวกับที่ core ออก แต่ **core จริงไม่รู้จัก** — ใช้ในเครื่องและในเทสต์เท่านั้น
- `dev:keys` ไม่เขียนทับกุญแจที่มีอยู่ (ใส่ `--force` เมื่อแน่ใจ)
- ปิด infra: `pnpm test:e2e:down` (ลบข้อมูลด้วย)

**ทดสอบ:** `pnpm test` (unit) · `pnpm test:e2e` (บูต service จริงแล้วคุยผ่าน Kafka เหมือน service อื่น)

### 3. ลงทะเบียน service กับ core (ระดับระบบ)

**ด้วยคำสั่ง** — ใช้ได้ทั้ง core ในเครื่องและบน server

```bash
cp .env.register.example .env.register   # กรอก GUMON_CORE_URL + credential (git ไม่เก็บไฟล์นี้)
pnpm gumon:register --dry-run           # ดูสิ่งที่จะส่ง (ซ่อนค่าลับ)
pnpm gumon:register                     # เรียก registerService แล้วเขียนกุญแจลง .gumon/certificates/<serviceKey>/
```

- ยืนยันตัวด้วย credential ชนิด `SYSTEM` (`GUMON_CLIENT_ID` + `GUMON_CLIENT_SECRET`) หรือ access token ของผู้ดูแล (`GUMON_ACCESS_TOKEN`) — ผู้เรียกต้องมีสิทธิ์ `registerService`
- service ที่มีหน้า admin ใส่ `GUMON_SERVICE_TYPE`, `GUMON_URL_FRONTEND`, `GUMON_URL_GET_METADATA` เพิ่ม

**หรือผ่านหน้าเว็บ** — system admin › Service › register (GraphQL [`registerService`](coreService.md#registerservice))
แล้วนำกุญแจไปวางใน `.gumon/certificates/<serviceKey>/` (`certificate-id.key`, `certificate`, `certificate.pub`, `hash.key`, `symmetric.key`)

ทั้งสองทาง: `serviceKey` = ค่าเดียวกับ `SERVICE_KEY` · `isCoreSet: false` · **กุญแจได้ครั้งเดียว ห้าม commit `.gumon/`**
ตอนเริ่มทำงาน service ส่ง `register-service` ประกาศตัวกับ core · ตรวจสถานะได้ที่ system admin › Service-ready

### 4. เพิ่ม service เข้าแอป (ระดับแอป)

ที่หน้า system admin › app › app-service › add (หรือ [`addServiceToApp`](coreService.md#addservicetoapp) กับ `refreshData: true`)

service จะได้รับ `sync-app-certificate`, `sync-app-credential`, `sync-application`, `sync-user-policy`, `sync-app-service-setting` ของแอปนั้น ⇒ ตรวจ token ของผู้ใช้ในแอปได้ทันที

### 5. ผูก permission กับ role

`refresh-data` ทำให้ service ส่ง `sync-permission` ให้ access-control → ผู้ดูแลผูก permission เข้ากับ role → access-control ส่ง `sync-user-policy` กลับมา ⇒ ผู้ใช้ที่มี role นั้นเรียก API ได้

### 6. เขียนงานของ service

เพิ่ม API ([API Reference](#api-reference)) และ topic ([Kafka](#kafka-consume-reference)) · ใช้บริการกลางผ่าน Kafka แทนการทำเอง (ตั้งเวลา → `set-schedule`, แจ้งเตือน → `create-notification`, ไฟล์ → `sync-file-upload`)

### 7. (ถ้ามี) หน้า admin

ทำหน้า admin ย่อยจาก `gumon-dynamic-admin-iframe-template` เปิด `GET /api/menuMetaData` แล้วลงทะเบียนพร้อม `urlFrontend`, `urlGetMetaData` ดู [เมนูของหน้า admin](frontends.md)

---

## API Reference

API ของ service เป็น GraphQL ที่ `POST /graphql` · หน้าบ้านเรียกได้ตรง · **service อื่นห้ามเรียก**

### Query / Mutation

1. เขียน schema ใน `src/graphqls/<module>/<module>.graphql` แล้ว `pnpm generate`
2. เพิ่ม permission ของ operation ใน `src/constants/permissionKey.ts` และรายละเอียดใน `permissions.ts`

```ts
// permissionKey.ts
export const PERMISSION_KEY = {
  getOrders: 'getOrders',
  createOrder: 'createOrder',
};

// permissions.ts
export const PERMISSIONS: IPermission[] = [
  {
    permissionKey: PERMISSION_KEY.createOrder,
    title: 'Create order',
    description: 'สร้างคำสั่งซื้อ',
    isGenerateApplication: true,   // ให้สิทธิ์ทั้งแอปได้ (ผ่าน appRole)
    isGenerateOrganization: false, // ให้สิทธิ์แยกตามองค์กรได้ (ผ่าน organizationRole)
    systemNote: '',
  },
];
```

3. resolver ใช้ `AuthGuard` (ต้อง login) หรือ `AppCredentialGuard` (ยอมรับ `X-APP-CLIENT-ID` สำหรับข้อมูลที่อ่านได้ก่อน login)
4. ใน service ตรวจสิทธิ์ก่อนทำงาน

```ts
await this.verifyUserPolicyService.verifyUserPolicy({
  appKey,
  authId,
  permissionKey: PERMISSION_KEY.createOrder,
  // organizationId,  // ถ้าตรวจระดับองค์กร
});
```

**ระดับแอป หรือ ระดับองค์กร**

| ตั้ง | ใช้เมื่อ | ตอนตรวจ |
| --- | --- | --- |
| `isGenerateApplication: true` | สิทธิ์นี้ใช้ได้ทั้งแอป (เช่น ผู้ดูแลแอปจัดการข้อมูลทุกองค์กร) | `verifyUserPolicy({ appKey, authId, permissionKey })` |
| `isGenerateOrganization: true` | สิทธิ์นี้ให้เฉพาะในองค์กร (เช่น พนักงานสาขา A จัดการได้แค่สาขา A) | ใส่ `organizationId` ขององค์กรที่ข้อมูลนั้นอยู่ |

ตั้ง `true` ทั้งคู่ได้ ⇒ ผู้ดูแลเลือกผูกได้ทั้งสองระดับ

**ข้อมูลของใครของมัน** (เช่น ผู้ป่วยเห็นเฉพาะนัดของตัวเอง) — permission ตอบได้แค่ "ทำ X ได้ไหม" ไม่ได้ตรวจว่าเป็นเจ้าของ
ให้ service กรองเองด้วย `authId` จาก token (`ctx.user.authId`) เช่น query `getMyAppointments` คืนเฉพาะรายการที่ `patientAuthId === authId` · ตั้งชื่อ API แบบ `getMy…` ให้รู้ว่าเป็นข้อมูลของคนที่ login

query ที่เป็นรายการใช้ input รูปเดียวกับ service อื่น `{ filter, search, sort: { sortBy, sortOrder }, pagination: { page, limit } }`

---

## kafka consume Reference

topic ที่ต้องรับแบ่งตามระดับใน [สิ่งที่ Service ต้องมี](serviceX.md) — ทุกตัว: `sync-app-certificate`, `refresh-data` · มี API: `sync-app-credential`, `sync-user-policy` · ตามการใช้งาน: `sync-application`, `sync-organization`, `sync-auth`, `sync-service-setting`, `sync-app-service-setting`, `schedule-alarm` · payload ดูที่ [Kafka topic มาตรฐาน](standardTopics.md)

เพิ่ม topic ที่รับ

1. เพิ่มชื่อใน `KAFKA_TOPIC_CONSUMER` (`src/constants/kafka/kafkaTopic.ts`)
2. สร้าง `src/kafka-consumer/<topic>/<topic>.module.ts` + `<topic>.service.ts` แล้ว import module ใน `app.module.ts`
3. ใน `onModuleInit` เรียก `await this.kafkaConsumerService.kafkaInitProcess(topic, this.processMessage.bind(this))` · consumer group จะเป็น `<serviceKey>-consumer-<topic>` · ต้อง `await` เพื่อให้บูตล้มชัด ๆ ถ้าต่อ Kafka ไม่ได้
4. ใน `processMessage(headers, message)`:
    - **เฉพาะ** topic ที่ส่งถึง service เดียว → ทิ้งถ้า `headers.serviceKey !== SERVICE_KEY` · topic กระจายทั้งแอป (`sync-application`, `sync-auth`, `sync-<entity>` ของ service อื่น ฯลฯ) **ห้ามกรอง** — ดูตารางใน [สิ่งที่ Service ต้องมี](serviceX.md#ฝงรบ-กรองขอความอยางไร)
    - ทิ้งถ้าไม่มี App Certificate ของตัวเองใน `headers.appKey` (ใช้ `kafkaConsumerHelper.getAppCertificate(appKey, SERVICE_KEY)`)
    - แยกงานตาม `action` (`ADD`, `REMOVE`, ...) แล้วบันทึกสำเนาลง DB ของตัวเอง โดยห่อด้วย `runInTransaction(connection, async (session) => { ... })` — commit / abort / ปิด session ให้เสมอ
    - ล้าง Redis key ที่เกี่ยวข้อง **หลัง** transaction commit แล้ว

ตัวอย่าง consumer ของข้อมูลที่ service อื่นประกาศ (กระจายทั้งแอป จึงไม่กรอง `serviceKey`)

```ts
async processMessage(headers: IKafkaHeaders, message: string) {
  const { action, appointment } = JSON.parse(message);   // JSON ผิดรูป → throw → ระบบลองซ้ำ/dead-letter ให้
  const appCertificate = await this.kafkaConsumerHelper.getAppCertificate(headers.appKey, SERVICE_KEY);
  if (!appCertificate) return null;                       // เราไม่ได้อยู่ในแอปนี้

  await runInTransaction(this.sectionConnection, async (session) => {
    if (action === 'ADD') {
      await this.appointmentModel.findByIdAndUpdate(appointment.id, appointment, { upsert: true, session });
    }
    if (action === 'REMOVE') {
      await this.appointmentModel.findByIdAndDelete(appointment.id, { session });
    }
  });
  await deleteRedisKeysWithPrefix({ prefix: `${SERVICE_KEY}:appointment:`, redis: this.redis }); // หลัง commit
}
```

**เมื่อประมวลผลไม่สำเร็จ ให้ throw ได้เลย** — `KafkaConsumerService` จัดการให้

| สถานการณ์ | ระบบทำอะไร |
| --- | --- |
| handler throw (ข้อมูลผิดรูป · DB ล่มชั่วคราว ฯลฯ) | ลองซ้ำแบบเว้นระยะ `KAFKA_MESSAGE_MAX_RETRY` ครั้ง (ค่าเริ่มต้น 3) |
| ยังไม่สำเร็จ | ส่งไป `KAFKA_DEAD_LETTER_TOPIC` (ถ้าตั้ง) พร้อม header บอก topic / offset / สาเหตุ แล้วไปข้อความถัดไป — ข้อความเสียข้อความเดียวไม่ทำให้ทั้ง topic ค้าง |
| consumer ล่มแบบกู้ไม่ได้ | ปิด process ให้ระบบ (เช่น k8s) เริ่มใหม่ — ไม่ปล่อยให้ service รันต่อโดยไม่รับ topic นั้น |

⛔ อย่า catch แล้วกลืน error เงียบ ๆ — ข้อมูลจะหายโดยไม่มีใครรู้

---

## Kafka Produce Reference

topic ที่ต้องส่ง: `register-service` + `hand-check-result` (ทุกตัว · template ทำให้แล้ว) · `sync-permission` (ถ้ามี API) · `sync-<entity>` ของข้อมูลที่ตัวเองเป็นเจ้าของ (ส่งเมื่อเปลี่ยน และส่งซ้ำทั้งชุดเมื่อได้ `refresh-data`) · payload ดูที่ [Kafka topic มาตรฐาน](standardTopics.md)

เพิ่ม topic ที่ส่ง: เพิ่มชื่อใน `KAFKA_TOPIC_PRODUCE` แล้วส่งด้วย

```ts
await this.kafkaProducerService.produceApp({
  topic: KAFKA_TOPIC_PRODUCE.setSchedule,
  headerAppKey: appKey,
  headerServiceKey: 'schedule', // service ปลายทาง ไม่ใช่ SERVICE_KEY ของตัวเอง
  dataArray: [{ action: 'ADD', schedule: { /* ... */ } }],
});
```

**ประกาศข้อมูลของตัวเองให้ service อื่น (`sync-<entity>`)** — กระจายทั้งแอป ใส่เฉพาะ `headerAppKey`

```ts
// ส่งทุกครั้งที่สร้าง / แก้ / ยกเลิก และส่งซ้ำทั้งชุดเมื่อได้ refresh-data
await this.kafkaProducerService.produceApp({
  topic: 'sync-appointment',
  headerAppKey: appKey,                       // ไม่ใส่ headerServiceKey = ถึงทุก service ในแอป
  dataArray: [{
    action: 'ADD',                            // ADD = สร้างหรือแก้ (ผู้รับ upsert ตาม id) · REMOVE = ลบออก
    appointment: { id, appKey, status: 'CANCELLED', patientAuthId, startAt },
  }],
});
```

- ข้อมูลที่ยังอยู่แต่เปลี่ยนสถานะ (เช่น ยกเลิกนัด) ส่ง `ADD` พร้อม `status` · ใช้ `REMOVE` เมื่อลบข้อมูลออกจริง
- payload ใส่ข้อมูลที่ service อื่นต้องใช้ให้ครบ ผู้รับจะเก็บสำเนาไว้ ไม่ย้อนมาถามเรา
- ชื่อ topic: `sync-<entity>` (kebab-case) · ประกาศ payload ไว้ใน README ของ service

---

## Checklist

- [ ] ตั้ง `SERVICE_KEY` และชื่อ package แล้ว (ไม่ใช่ `CHANGE_ME`)
- [ ] `.gumon/certificates/<serviceKey>/` เป็นกุญแจจาก core (ไม่ใช่กุญแจจาก `dev:keys`) และอยู่ใน `.gitignore`
- [ ] ส่ง `register-service` ตอนเริ่ม
- [ ] รับ `sync-app-certificate` และ `refresh-data` (ทุกตัว) · รับ `sync-app-credential` และ `sync-user-policy` (ถ้ามี API)
- [ ] `refresh-data`: กันทำซ้ำด้วย `refreshDataId`, ส่ง `sync-permission` + ข้อมูลของตัวเองใหม่, ล้าง Redis `<serviceKey>:`
- [ ] ทุก API ตรวจ permission ผ่าน UserPolicy และทุก permission อยู่ใน `permissions.ts`
- [ ] header `serviceKey` = service ปลายทาง ทุกข้อความที่ส่ง
- [ ] ไม่เรียก API ของ service อื่นตรง · ไม่ตั้ง cron เอง (ใช้ schedule)
- [ ] consumer เขียน DB ผ่าน `runInTransaction` และไม่กลืน error
- [ ] บน server: `GRAPHQL_PLAYGROUND` ไม่ตั้งหรือเป็น `false` · `BY_PASS_ACL='false'` · ตั้ง `KAFKA_DEAD_LETTER_TOPIC`
- [ ] `pnpm test` และ `pnpm test:e2e` ผ่าน

---

> อัปเดตจากโค้ด gumon-backend-template@f1ae0e8 · 2026-10-06
