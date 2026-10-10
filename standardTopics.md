# Kafka topic มาตรฐาน

service ทุกตัวใน Gumon คุยกันผ่าน Kafka (ดู [แนวคิด Gumon](concept.md#kafka-only))
topic ใน Gumon แบ่ง 4 กลุ่ม — หน้านี้รวม 3 กลุ่มแรก กลุ่มสุดท้ายอยู่ในหน้าของแต่ละ service

| กลุ่ม | คืออะไร | service ใหม่ต้องสนใจไหม |
|---|---|---|
| [topic มาตรฐาน](#engine-topics) | สัญญากลางระหว่าง core set กับ **ทุก** service | ✔ ตามระดับใน [สิ่งที่ Service ต้องมี](serviceX.md) |
| [topic ภายใน core set](#core-set-internal) | core set ใช้ประสานกันเอง | ✘ แค่รู้ว่ามี |
| [topic บริการกลาง](#central-topics) | เรียกใช้ schedule · notification · storage · เลขรัน | เมื่อต้องใช้บริการนั้น |
| [topic ของแต่ละ service](#per-service) | ข้อมูลธุรกิจเฉพาะของ service นั้น (`sync-<entity>` ฯลฯ) | เมื่อต้องใช้ข้อมูลของ service นั้น |

ชื่อ topic ในโค้ดเป็น kebab-case (`sync-app-certificate`) บางครั้งเรียกแบบ camelCase (`syncAppCertificate`) — คือตัวเดียวกัน

- [รูปแบบข้อความ](#message-format)
- [topic มาตรฐานของ engine](#engine-topics)
- [topic บริการกลางที่ service ธุรกิจใช้](#central-topics)
- [topic ของแต่ละ service](#per-service)

---

<br>

<a id="message-format"></a>

## รูปแบบข้อความ

- header: `appKey` (ข้อมูลของแอปไหน) + `serviceKey` (**service ปลายทาง** · ไม่ใส่ = ส่งถึงทุกตัว)
- payload รูปเดียวกันทุก topic: `{ action, <entity>: {...} }` โดย `action` เป็นเช่น `ADD` · `REMOVE` · `REMOVE_APP`
- consumer group ตั้งชื่อ `${SERVICE_KEY}-consumer-${topic}` ⇒ หลาย replica ของ service เดียวกันแบ่งงานกัน ข้อความหนึ่งถูกประมวลผลครั้งเดียว
- ก่อนทำงานกับข้อความของแอปใด service ตรวจว่าตัวเองมี AppCertificate ของแอปนั้น (ถูกเพิ่มเข้าแอปแล้ว) ไม่มี = ไม่รับ

---

<br>

<a id="engine-topics"></a>

## topic มาตรฐานของ engine

คอลัมน์ "ทุก service ต้องรับ/ส่ง" หมายถึง service ธุรกิจตัวใหม่ต้องทำ topic นี้ด้วยหรือไม่

| topic (โค้ด) | ชื่อเรียก | ผู้ส่ง → ผู้รับ | ใช้ทำอะไร | ทุก service ต้องรับ/ส่ง |
|---|---|---|---|---|
| `init-system` | initSystem | core → core set | ตั้งระบบจากศูนย์: สร้างแอป `SYSTEM` แล้วให้ core set แต่ละตัวสร้างข้อมูลตั้งต้นของตัวเอง | ไม่ — **เฉพาะ core set** |
| `refresh-data` | refreshData | core → ทุก service (header `serviceKey` = ปลายทาง) | สั่งให้ service ส่งข้อมูลที่ตัวเองเป็นเจ้าของขึ้นไปใหม่ (รวม `sync-permission`) เพื่อให้ service ที่เพิ่งเข้ามาทำงานต่อได้ · และล้าง cache Redis ของตัวเอง | **ต้องรับ** |
| `health-check` → `health-check-result` | healthCheck | core → service → core | ถามว่า service ยังทำงานอยู่ไหม | ไม่บังคับ |
| `hand-check` → `hand-check-result` | handCheck / handCheckResult | core → service → core | ถามว่าปลายทางยังถือกุญแจ (SystemCertificate) ถูกต้องไหม | ไม่บังคับ |
| `register-service` | registerService | service → core | service ลงทะเบียน/ประกาศตัวเองกับ core ผ่าน Kafka · core ตอบด้วย `hand-check` | แนะนำ |
| `sync-permission` | syncPermission | ทุก service → access-control (header `serviceKey: access-control`) | service ประกาศ permission ของตัวเองให้ ACL รู้ เพื่อนำไปผูกกับ role | **ต้องส่ง** (ตอนได้ `refresh-data`) |
| `sync-app-certificate` | syncAppCertificate | core → ทุก service | แจก AppCertificate ของ service ในแอป (privateKey เข้ารหัสด้วย SystemCertificate ของปลายทาง) · เป็นตัวบอกว่า service ไหนอยู่ในแอปไหน | **ต้องรับ** |
| `sync-app-credential` | syncAppCredential | authentication → ทุก service | ข้อมูลการเข้าใช้ของแอป: `clientId` · key ตรวจ token (`jwtAccessSecretKey`, `jwtRefreshSecretKey`) · กฎของ token · รวม credential type `SYSTEM` (= apiKey สำหรับระบบภายนอก: `clientId` + `clientSecret`) | **ต้องรับ** |
| `sync-application` | syncApplication | application → ทุก service | ข้อมูลแอป (เพิ่ม/แก้/ลบ) · core ใช้ topic นี้ผูก core set เข้าแอปใหม่อัตโนมัติ | **ต้องรับ** |
| `sync-organization` | syncOrganization | unit → service ที่ใช้ organization | ข้อมูล organization | รับถ้า service ใช้ org |
| `sync-user-policy` | syncUserPolicy | access-control → service เจ้าของ permission (header `serviceKey` = ปลายทาง) | UserPolicy ที่คอมไพล์แล้ว พร้อมใช้ตรวจสิทธิ์ (ดูรูป key ด้านล่าง) | **ต้องรับ** |
| `sync-service-setting` | syncServiceSetting | core → service | JSON ตั้งค่าเพิ่มเติมของ service **ทั้งระบบ** แก้ผ่าน core ได้โดยไม่ต้องแก้ env | รับถ้ามีค่าตั้ง |
| `sync-app-service-setting` | syncAppServiceSetting | core → service | JSON ตั้งค่าเพิ่มเติมของ service **เฉพาะแอป** (เช่น ค่า OAuth ของ login ผ่าน Google/Facebook/Apple ใน authentication) | รับถ้ามีค่าตั้งต่อแอป |
| `sync-auth` | syncAuth | authentication → service ที่ต้องใช้ | profile ของ user ที่ authentication ถือ (ชื่อ, `defaultOrganizationKey`) | ไม่บังคับ แต่ใช้บ่อย |

### รูป key ของ UserPolicy

service รับ `sync-user-policy` มาเก็บไว้ใน DB/Redis ของตัวเอง ตอนรับ request ก็สร้าง key แล้วค้นได้ทันที ไม่ต้องถาม ACL

```
${SERVICE_KEY}::${permissionKey}:${appKey}::${authId}                   สิทธิ์ระดับแอป
${SERVICE_KEY}::${permissionKey}:${appKey}:${organizationId}:${authId}  สิทธิ์ระดับองค์กร
```

<a id="core-set-internal"></a>

### topic ที่ core set ใช้ประสานกันเอง

**ไม่ใช่ topic มาตรฐาน** — service ธุรกิจไม่ต้องรับ แต่ควรรู้ว่ามีอยู่

| topic | ผู้ส่ง → ผู้รับ | ใช้ทำอะไร |
|---|---|---|
| `sync-service` | core → access-control | ทะเบียน service (ชนิด, `urlFrontend`, `urlGetMetaData`) ให้ ACL ดึงเมนูของหน้า admin ย่อย (ดู [หน้าบ้าน](frontends.md#menu-metadata)) |
| `add-admin-app-role` | core → unit | แอปใหม่ ⇒ unit สร้าง role `admin` ของแอป |
| `sync-profile` | profile → service ที่ใช้ profile | ข้อมูล profile หลังแก้ไข |

---

<br>

<a id="central-topics"></a>

## topic บริการกลางที่ service ธุรกิจใช้

service ธุรกิจใช้ความสามารถของ core set ผ่าน topic เหล่านี้ — **header `serviceKey` ใส่ key ของบริการปลายทาง**

| บริการ | ส่งขอ (serviceKey) | ผลที่ได้กลับ | ใช้ทำอะไร |
|---|---|---|---|
| ตั้งเวลา | `set-schedule` `{ADD/REMOVE, schedule}` (`schedule`) | `schedule-alarm` เมื่อถึงเวลา (header `serviceKey` = ผู้ขอ) → ผู้ขอตอบ `schedule-alarm-result` | นัดครั้งเดียวหรือวนซ้ำ · ฝาก `alarmData` ไปกับนัดได้ · service จึงไม่ต้องมี cron ของตัวเองและรันหลาย replica ได้โดยไม่ยิงซ้ำ — [Schedule Service](scheduleService.md) |
| แจ้งเตือน | `create-notification` (`notification`) | `create-notification-result` | แจ้งเตือน in-app / email / SMS · ตั้งเวลาส่งได้ — [Notification Service](notificationService.md) |
| ไฟล์ | `sync-file-upload` (`storage`) | — | ลงทะเบียนไฟล์ที่ service เขียนลง storage เอง (ปกติหน้าบ้านขอ presigned URL จาก GraphQL ของ storage ตรง) — [Storage Service](storageService.md) |
| ประกาศ permission | `sync-permission` (`access-control`) | `sync-user-policy` | ดูตารางด้านบน — [ACL Service](aclService.md) |
| เลขรันนิ่ง | `register-custom-running-number` · `generate-running-number` (`unit`) | `sync-custom-running-number` · `generated-running-number-result` | ขอเลขเอกสารต่อเนื่องตามรูปแบบที่กำหนด |

---

<br>

<a id="per-service"></a>

## topic ของแต่ละ service

topic ที่เป็นข้อมูลเฉพาะของ service หนึ่ง (เช่น `sync-organization` ของ unit · `sync-auth` ของ authentication) อยู่ในหน้าของ service นั้น หัวข้อ **kafka consume** (รับ) และ **Kafka Produce** (ส่ง)

| service | topic ที่รับ | topic ที่ส่ง |
|---|---|---|
| [Core](coreService.md) | [รับ](coreService.md#kafka-consume-reference) | [ส่ง](coreService.md#kafka-produce-reference) |
| [Application](applicationService.md) | [รับ](applicationService.md#kafka-consume-reference) | [ส่ง](applicationService.md#kafka-produce-reference) |
| [Authentication](authenticationService.md) | [รับ](authenticationService.md#kafka-consume-reference) | [ส่ง](authenticationService.md#kafka-produce-reference) |
| [ACL](aclService.md) | [รับ](aclService.md#kafka-consume-reference) | [ส่ง](aclService.md#kafka-produce-reference) |
| [Unit](unitService.md) | [รับ](unitService.md#kafka-consume-reference) | [ส่ง](unitService.md#kafka-produce-reference) |
| [Profile](userService.md) | [รับ](userService.md#kafka-consume-reference) | [ส่ง](userService.md#kafka-produce-reference) |
| [Notification](notificationService.md) | [รับ](notificationService.md#kafka-consume-reference) | [ส่ง](notificationService.md#kafka-produce-reference) |
| [Schedule](scheduleService.md) | [รับ](scheduleService.md#kafka-consume-reference) | [ส่ง](scheduleService.md#kafka-produce-reference) |
| [Storage](storageService.md) | [รับ](storageService.md#kafka-consume-reference) | [ส่ง](storageService.md#kafka-produce-reference) |

---

> อัปเดตจากคำผู้พัฒนาและโค้ด gumon-tech · 2026-10-05
