# แนวคิด Gumon

Gumon คือระบบแบบ **Composable System = Microservices + Event-Driven**
แอปหนึ่งตัวเกิดจากการหยิบ service สำเร็จรูปหลายตัวมาประกอบกัน
service ตัวเดียวให้บริการได้หลายแอปพร้อมกัน · สร้างแอปใหม่จาก service ที่มีอยู่ได้ทันที · เพิ่ม service ใหม่ได้โดยไม่กระทบของเดิม

- [3 ชั้นของระบบ](#3-layers)
- [กฎ: service คุยกันผ่าน Kafka เท่านั้น](#kafka-only)
- [app และ organization](#tenancy)
- [header appKey และ serviceKey](#headers)
- [core set กับ service ธุรกิจ](#core-set)

---

<br>

<a id="3-layers"></a>

## 3 ชั้นของระบบ

```
Data Stream Layer (Kafka)  ◀──▶  API Service Layer  ◀──▶  Application Layer
event streaming / data sync      core set + service ธุรกิจ     หน้าบ้าน: admin · เว็บ · มือถือ · kiosk
```

| ชั้น | หน้าที่ |
|---|---|
| **Data Stream Layer** | Kafka เป็นทางเดินข้อมูลระหว่าง service ทั้งหมด ทั้ง event และการ sync ข้อมูล |
| **API Service Layer** | service พร้อมใช้ แต่ละตัวมี GraphQL API ของตัวเอง ฐานข้อมูลของตัวเอง และรองรับหลายแอป |
| **Application Layer** | หน้าบ้านทุกชนิด เรียก GraphQL ของแต่ละ service ตรง ดู [หน้าบ้าน (Admin UI)](frontends.md) |

---

<br>

<a id="kafka-only"></a>

## กฎ: service ↔ service คุยกันผ่าน Kafka เท่านั้น

- service **ห้าม** เรียก API (GraphQL/HTTP) ของ service อื่นตรง ๆ ให้ส่งและรับข้อมูลผ่าน Kafka topic
- **หน้าบ้าน** เรียก GraphQL API ของแต่ละ service ได้ตรง

### ทำไมต้องเป็น event

ถ้า service A เรียก service B ตรง ๆ วันที่มี service C อยากรู้เหตุการณ์เดียวกันเพื่อไปทำงานของตัวเองต่อ **ต้องกลับไปแก้ A**
ถ้า A ปล่อย event ออกมา C แค่ subscribe topic นั้นเพิ่ม **ของเดิมไม่ต้องแตะเลย**

ผลที่ได้ตามมา

- แต่ละ service เก็บ **สำเนา** ข้อมูลที่ตัวเองต้องใช้ (ข้อมูลแอป, organization, สิทธิ์ผู้ใช้ ฯลฯ) ไว้ใน DB/Redis ของตัวเอง ⇒ ไม่ต้องรอถามใคร ทำงานต่อได้แม้ต้นทางไม่อยู่
- service ที่เข้ามาทีหลังตามข้อมูลเดิมทันได้ด้วย topic `refresh-data` (ดู [วงจรชีวิตของ service](serviceLifecycle.md))
- ตรวจสิทธิ์ได้ในตัว เพราะ UserPolicy ถูกส่งมาให้ล่วงหน้าผ่าน `sync-user-policy` ไม่ต้องถาม service อื่นตอนรับ request
- service เดียวกันรันหลาย replica ได้ — consumer group ของ Kafka ส่งแต่ละข้อความให้ replica เดียว

สิ่งที่ต้องออกแบบรองรับ: ข้อมูลระหว่าง service เป็นแบบ eventually consistent (สำเนาจะตามมาภายหลังเล็กน้อย)

---

<br>

<a id="tenancy"></a>

## app และ organization

```
app (appKey) ─┬─ organization (organizationId / orgKey) ─ องค์กรลูก
              └─ user (authId) — บัญชีผูกกับแอป
```

| ระดับ | ความหมาย |
|---|---|
| **app** | โซลูชันหนึ่งชุด มีธีม โดเมน และ credential ของตัวเอง · ระบุด้วย `appKey` · แอป `SYSTEM` ถูกสร้างตอนตั้งระบบครั้งแรก |
| **organization** | หน่วยงานภายในแอป จัดเป็นต้นไม้ (มีองค์กรลูกได้) · role ระดับองค์กรกำหนดสิทธิ์ลงองค์กรลูกได้ |
| **user** | บัญชีผู้ใช้ผูกกับแอป — คนเดียวใช้ 2 แอปคือ 2 บัญชี |

ทุก service เก็บข้อมูลของทุกแอปไว้รวมกัน โดยทุก record มี `appKey` กำกับ

---

<br>

<a id="headers"></a>

## header appKey และ serviceKey

ข้อความ Kafka ระหว่าง service มี header 2 ตัว

| header | ความหมาย |
|---|---|
| `appKey` | ข้อความนี้เป็นข้อมูลของแอปไหน |
| `serviceKey` | **service ปลายทาง** ที่ข้อความนี้ส่งถึง (ไม่ใส่ = ส่งถึงทุก service) |

> ⚠️ `serviceKey` คือ **ปลายทาง** ไม่ใช่ตัวผู้ส่ง เช่น service ธุรกิจส่ง `set-schedule` ให้ใส่ `serviceKey: schedule` · ส่ง `create-notification` ใส่ `notification` · ส่ง `sync-permission` ใส่ `access-control` · ส่ง `sync-file-upload` ใส่ `storage`

service ปลายทางรับข้อความของแอปใดได้ก็ต่อเมื่อตัวเองถูกเพิ่มเข้าแอปนั้นแล้ว (มี AppCertificate ของแอปนั้น) ดู [วงจรชีวิตของ service](serviceLifecycle.md#app-level)

---

<br>

<a id="core-set"></a>

## core set กับ service ธุรกิจ

ตอนลงทะเบียน service มีค่า `isCoreSet`

- `isCoreSet: true` — service ที่ระบบต้องมีเพื่อให้ทำงานได้ **โดยยังไม่มีธุรกิจใดเกี่ยวข้อง** · ถูกตั้งขึ้นพร้อมกันตอน `init-system` และถูกผูกเข้าทุกแอปใหม่อัตโนมัติ
- `isCoreSet: false` — **service ธุรกิจ** (ค่าเริ่มต้น) เช่น CMS, content หรือ service ที่ทีมสร้างเองตามงาน · ลงทะเบียนเข้าระบบและเพิ่มเข้าแอปที่ต้องใช้ทีละแอป

### core set

| serviceKey | หน้าที่ |
|---|---|
| `core` | ทะเบียน service · ผู้ออกกุญแจ (SystemCertificate / AppCertificate) · ค่าตั้ง service · สั่ง `refresh-data` · `init-system` — [Core Service](coreService.md) |
| `authentication` | บัญชีผู้ใช้ · login ทุกแบบ · token · เจ้าของ AppCredential — [Authentication Service](authenticationService.md) |
| `application` | ข้อมูลแอปและธีม · HostTheme (โดเมน → แอป) — [Application Service](applicationService.md) |
| `access-control` | ทะเบียน permission · role · Custom Menu · คอมไพล์ UserPolicy แล้วกระจาย — [ACL Service](aclService.md) |
| `unit` | organization (ต้นไม้) · นิยาม role · running number · รวมงาน label เดิมไว้ด้วย |
| `profile` | ข้อมูลส่วนตัวผู้ใช้ · ฟิลด์เสริมที่แอปกำหนดเอง |
| `notification` | แจ้งเตือน in-app · email · SMS — [Notification Service](notificationService.md) |
| `schedule` | ตัวกลางตั้งเวลา (cron) ของทั้งระบบ — [Schedule Service](scheduleService.md) |
| `storage` | ไฟล์ · presigned URL · metadata — [Storage Service](storageService.md) |

> `label` เดิมถูกยุบรวมเข้า `unit` แล้ว ไม่นับเป็น service แยก

service ธุรกิจใช้ความสามารถของ core set ผ่าน topic เท่านั้น ดูรายการที่ [Kafka topic มาตรฐาน](standardTopics.md)

---

> อัปเดตจากคำผู้พัฒนาและโค้ด gumon-tech · 2026-10-05
