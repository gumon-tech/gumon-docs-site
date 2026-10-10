# สิ่งที่ Service ต้องมี

**ข้อจำกัดเดียวของการต่อเข้า Gumon: service ต้องคุยผ่าน Kafka ได้**
จะเขียนด้วยภาษาอะไร ใช้ฐานข้อมูลอะไรก็ได้ ขอแค่รับ–ส่งข้อความ Kafka ตามกติกาในหน้านี้

หน้านี้รวมเฉพาะสิ่งที่จำเป็นจริง แบ่ง 3 ระดับ — เริ่มจากระดับ 1 แล้วเพิ่มเท่าที่ service ต้องใช้
ถ้าเริ่มจาก `gumon-backend-template` จะได้ทั้งหมดนี้มาแล้ว ดูขั้นตอนที่ [สร้าง Service ใหม่](createService.md)

| ระดับ | เมื่อไร | รับ (consume) | ส่ง (produce) |
| --- | --- | --- | --- |
| **1 · ต้องมีทุกตัว** | ทุก service | `sync-app-certificate` · `refresh-data` · `hand-check` | `register-service` · `hand-check-result` |
| **2 · มี API ให้ผู้ใช้** | service มี GraphQL/REST ที่ต้อง login | `sync-app-credential` · `sync-user-policy` | `sync-permission` |
| **3 · ใช้เมื่อจำเป็น** | ต้องการข้อมูลหรือบริการกลาง | `sync-application` · `sync-organization` · `sync-auth` · `sync-service-setting` · `sync-app-service-setting` · `schedule-alarm` | `set-schedule` · `create-notification` · `sync-file-upload` · `sync-<ข้อมูลของตัวเอง>` |

---

## กติกาการส่งข้อความ

**ห้ามยิง API ตรงระหว่าง service** — ส่ง event ผ่าน Kafka แทน
เหตุผล: ถ้า A เรียก B ตรง วันที่ C อยากรับเหตุการณ์เดียวกัน ต้องไปแก้ A · ถ้าเป็น event, C แค่ subscribe เพิ่ม
(หน้าบ้านเรียก GraphQL ของ service ได้ตรงตามปกติ)

| เรื่อง | กติกา |
| --- | --- |
| header `appKey` | แอปที่ข้อมูลนี้เป็นของ |
| header `serviceKey` | **service ปลายทาง** — ไม่ใช่ตัวผู้ส่ง (ข้อความกระจายทั้งแอปใส่แค่ `appKey`) |
| payload | JSON `{ "action": "ADD" \| "REMOVE" \| ..., "<entity>": { ... } }` |
| ชื่อ topic | kebab-case เช่น `sync-app-credential` |
| consumer group | `<serviceKey>-consumer-<topic>` ⇒ replica หลายตัวของ service เดียวกันแบ่งงานกัน ข้อความหนึ่งทำครั้งเดียว |

### ฝั่งรับ: กรองข้อความอย่างไร

ข้อความมี 2 แบบ — **กรอง `serviceKey` เฉพาะแบบส่งถึง service เดียว** ถ้าไปกรองแบบกระจายด้วย จะทิ้งข้อมูลที่ควรได้

| แบบ | topic | header `serviceKey` | ฝั่งรับทำอะไร |
| --- | --- | --- | --- |
| **ส่งถึง service เดียว** | `refresh-data` · `hand-check` · `sync-app-credential` · `sync-user-policy` · `sync-service-setting` · `sync-app-service-setting` · `schedule-alarm` · ทุกข้อความที่เราส่งหาบริการกลาง | key ของปลายทาง | ไม่ใช่ของเรา ⇒ **ทิ้ง** |
| **กระจายทั้งแอป** | `sync-application` · `sync-organization` · `sync-auth` · `sync-profile` · `sync-<entity>` ของ service อื่น | ว่าง | **ห้ามกรอง** `serviceKey` |
| **พิเศษ** | `sync-app-certificate` | service เจ้าของใบรับรอง | **ห้ามกรอง** — เก็บใบของทุก service ในแอป (ใช้ public key เข้ารหัสข้อมูลถึง service นั้น) · ถอด private key เฉพาะใบที่เป็นของเรา |

ทั้งสองแบบ: ข้อความของแอปที่เราไม่มี App Certificate ⇒ ทิ้ง (เราไม่ได้อยู่ในแอปนั้น)

---

## ระดับ 1 · ต้องมีทุกตัว

### กุญแจของ service (`.gumon`)

ได้มาตอนลงทะเบียน service กับ core อยู่ที่ `.gumon/certificates/<serviceKey>/` · service อ่านตอนบูต · **ห้าม commit ลง git**

### `register-service` (ส่ง) และ `hand-check` (รับ)

template ทำทั้งสองอย่างให้แล้ว — พิสูจน์กับ core ว่าเราถือกุญแจที่ core ออกให้ ⇒ core ขึ้นสถานะ Service-ready

1. ตอนบูต ส่ง `register-service` หา core (header `serviceKey: core`)
2. core ตอบ `hand-check` ที่มีค่าอ้างอิงเข้ารหัสด้วยกุญแจของเรา
3. ถอดด้วยไฟล์ `certificate` แล้วตอบ `hand-check-result` (header `serviceKey: core`) — ถอดไม่ได้ให้ส่ง `resultData: ""`

payload ของ `register-service`

| field | ค่า |
| --- | --- |
| `systemCertificateId` | จากไฟล์ `certificate-id.key` |
| `serviceKey` | key ของ service เรา |
| `encryptData` | `systemCertificateId` เข้ารหัสด้วย `certificate.pub` |

### `sync-app-certificate` (รับ)

บอกว่า service ไหนอยู่ในแอปไหน — **ไม่มี certificate ของเราในแอปนั้น = เราไม่มีสิทธิ์รับข้อมูลแอปนั้น**

- `ADD` → เก็บ `{ id, appKey, serviceKey, publicKey, ... }` ของทุก service (ใช้ `publicKey` เข้ารหัสข้อมูลที่ส่งถึง service นั้น)
- `REMOVE` → ลบ · ถ้าเป็นของเราเอง แปลว่าเราถูกถอดออกจากแอป

### `refresh-data` (รับ)

core สั่งให้ส่งข้อมูลที่เราเป็นเจ้าของขึ้น Kafka ใหม่ — ให้ service ที่เพิ่งเข้าแอปได้ข้อมูลครบ

1. ข้ามถ้าเคยทำ `refreshDataId` นี้แล้ว
2. ส่ง `sync-permission` และ `sync-*` ทุกอย่างที่เราเป็นเจ้าของในแอปนั้นใหม่
3. ล้าง cache ของตัวเอง (Redis key ที่ขึ้นต้นด้วย `<serviceKey>:`)

---

## ระดับ 2 · มี API ให้ผู้ใช้

### ยืนยันตัวตน

| ผู้เรียก | header | ตรวจกับ |
| --- | --- | --- |
| ผู้ใช้ | `Authorization: Bearer <accessToken>` | กุญแจ JWT ของแอป จาก `sync-app-credential` |
| ระบบภายนอก (apiKey) | `X-APP-CLIENT-ID` + `X-APP-CLIENT-SECRET` | credential ชนิด `SYSTEM` จาก `sync-app-credential` |
| ยังไม่ login (หน้า login · สาธารณะ) | `X-APP-CLIENT-ID` | บอกว่าเป็นแอปไหน — เมื่อมี token แล้วไม่ต้องส่ง เพราะ token มี clientId + appKey อยู่แล้ว |

### ตรวจสิทธิ์

```
service ──sync-permission──▶ access-control      ประกาศ permission ของเรา (serviceKey: access-control)
ผู้ดูแลผูก permission เข้ากับ role
access-control ──sync-user-policy──▶ service     UserPolicy พร้อมใช้ (เก็บไว้ใน DB/Redis ของเรา)
```

ตอนเรียก API: สร้าง key แล้วค้นในที่เก็บของเรา — เจอ = มีสิทธิ์

```
ระดับแอป:    ${SERVICE_KEY}::${permissionKey}:${appKey}::${authId}
ระดับองค์กร: ${SERVICE_KEY}::${permissionKey}:${appKey}:${organizationId}:${authId}
```

`sync-permission` หนึ่งข้อความต่อหนึ่ง permission: `{ serviceKey, permissionKey, title, description, isGenerateApplication, isGenerateOrganization, isActive }`

---

## ระดับ 3 · ใช้เมื่อจำเป็น

| ต้องการ | ทำอย่างไร |
| --- | --- |
| ข้อมูลแอป · องค์กร · ผู้ใช้ | รับ `sync-application` · `sync-organization` · `sync-auth` แล้วเก็บสำเนาไว้ใช้เอง · `sync-auth` มีชื่อ เบอร์โทร (`phoneNumber`) อีเมล ของผู้ใช้ — ใช้ส่ง SMS / อีเมลหาผู้ใช้ได้โดยไม่ต้องให้กรอกซ้ำ |
| ค่าตั้งค่าที่แก้ได้โดยไม่ต้องแก้ env | รับ `sync-service-setting` (ทั้งระบบ) · `sync-app-service-setting` (ต่อแอป) |
| ตั้งเวลา / งานตามรอบ | ส่ง `set-schedule` (`serviceKey: schedule`) → รับ `schedule-alarm` เมื่อถึงเวลา — **ใช้แทน cron ในตัว** service จึงรันหลาย replica ได้โดยงานไม่ซ้ำ · [Schedule Service](scheduleService.md) |
| ส่งแจ้งเตือน in-app / email / SMS | ส่ง `create-notification` (`serviceKey: notification`) · [Notification Service](notificationService.md) |
| เก็บไฟล์ | อัปโหลดผ่าน storage แล้วส่ง `sync-file-upload` (`serviceKey: storage`) · [Storage Service](storageService.md) |
| ให้ service อื่นใช้ข้อมูลของเรา | ประกาศ `sync-<entity>` ของตัวเอง ส่งเมื่อข้อมูลเปลี่ยน และส่งซ้ำทั้งชุดเมื่อได้ `refresh-data` |
| มีหน้า admin ฝังใน dynamic admin | เปิด `GET /api/menuMetaData` คืนรายการเมนู แล้วลงทะเบียนพร้อม `urlFrontend` + `urlGetMetaData` · ดู [หน้าบ้าน](frontends.md) |

payload ของทุก topic ดูที่ [Kafka topic มาตรฐาน](standardTopics.md)

---

> อัปเดตจากคำผู้พัฒนาและโค้ด gumon-backend-template@6e26695 · gumon-core-service@d872cc3 · 2026-10-05
