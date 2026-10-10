# วงจรชีวิตของ service

service หนึ่งตัวจะทำงานกับข้อมูลของแอปใดได้ ต้องผ่าน 2 ระดับ: **เข้าระบบ** (ระดับระบบ) แล้ว **เข้าแอป** (ระดับแอป)
หน้านี้อธิบายลำดับตั้งแต่ตั้งระบบจากศูนย์ การเสียบ service เข้า-ออก และกลไก "ตามทัน" ข้อมูล

- [ตั้งระบบจากศูนย์ (init-system)](#init-system)
- [ระดับระบบ: register / resign](#system-level)
- [ระดับแอป: เพิ่ม / ถอด service ในแอป](#app-level)
- [refresh-data: ตามข้อมูลให้ทัน](#refresh-data)
- [กุญแจ 3 ชั้น (ภาพรวม)](#keys)
- [ลำดับการเสียบ service ใหม่](#plug-in)

---

<br>

<a id="init-system"></a>

## ตั้งระบบจากศูนย์ (`init-system`)

ทำครั้งเดียวตอนติดตั้งระบบ (ผ่าน [Gumon CLI](gumoncli.md) หรือชุด docker ของ core set) — **เกี่ยวเฉพาะ core set**

1. core สร้างแอป `SYSTEM` และผู้ใช้ผู้ดูแลระบบคนแรก
2. core ลงทะเบียน service ใน core set ทุกตัว พร้อมออก SystemCertificate ให้ทีละตัว
3. core ผูก core set ทุกตัวเข้าแอป `SYSTEM` (AppCertificate · AppCredential · ค่าตั้ง service)
4. core ส่ง `init-system` ⇒ core set แต่ละตัวสร้างข้อมูลตั้งต้นของตัวเอง
5. ได้กุญแจของแต่ละ service ไว้ติดตั้งคู่กับ service นั้น จากนั้นรันทุกตัวตามปกติ

หลังจากนี้ ทุกแอปใหม่ที่สร้าง (`sync-application`) core จะผูก core set ทุกตัวเข้าแอปนั้นให้อัตโนมัติ และ unit สร้าง role `admin` ของแอปให้ (`add-admin-app-role`)

---

<br>

<a id="system-level"></a>

## ระดับระบบ: `registerService` / `resignService`

ทำที่ **system-admin › Service** (GraphQL ของ core)

| | ผล |
|---|---|
| **register** | service ถูกบันทึกในทะเบียนของระบบ · core ออก **SystemCertificate** ใหม่ให้ service นั้น (ได้รับครั้งเดียวตอนลงทะเบียน เก็บไว้กับ service ห้าม commit) · ระบุ `isCoreSet` (service ธุรกิจ = `false`) และ URL หน้า admin ย่อยถ้ามี |
| **resign** | ลบข้อมูล service นั้นออกจากระบบ · ระบบไม่ส่งข้อมูลใดถึง service นั้นอีก · ถ้าจะกลับมาต้อง register ใหม่และได้กุญแจชุดใหม่ |

service ต้องถอดออกจากทุกแอปก่อน resign และ service ใน core set ถอดออกไม่ได้

service ลงทะเบียน/ประกาศตัวผ่าน Kafka ได้ด้วย topic `register-service` (ดู [Kafka topic มาตรฐาน](standardTopics.md))

---

<br>

<a id="app-level"></a>

## ระดับแอป: `addServiceToApp` / `removeServiceFromApp`

ทำที่ **system-admin › app › app-service**

- **add** — core สร้าง **AppCertificate** ของ service ในแอปนั้นแล้วส่งผ่าน `sync-app-certificate` · คัดลอกค่าตั้งของ service มาเป็นค่าตั้งของแอป · ถ้าเลือก refresh data core จะส่ง `refresh-data` ให้ทุก service ในแอป ⇒ service ที่เพิ่งเข้ามาได้ข้อมูลเดิมของแอปครบ
- **remove** — ลบ AppCertificate ของ service ในแอปนั้น ⇒ service รับข้อมูลของแอปนั้นไม่ได้อีก

**AppCertificate เป็นตัวตัดสินว่า service เห็นแอปไหนได้** — service ตรวจทุกข้อความว่ามี AppCertificate ของตัวเองในแอปนั้นก่อนทำงาน ไม่มี = ไม่รับ
ดังนั้น service ที่ไม่ได้ถูกเพิ่มเข้าแอปใด ใช้ข้อมูลของแอปนั้นไม่ได้ และใช้ข้ามแอปไม่ได้

---

<br>

<a id="refresh-data"></a>

## `refresh-data`: ตามข้อมูลให้ทัน

เพราะแต่ละ service เก็บสำเนาข้อมูลที่ตัวเองต้องใช้ไว้เอง service ที่เพิ่งเข้ามาจึงต้องได้ข้อมูลเดิมก่อนจะทำงานได้

1. core ส่ง `refresh-data` (header `serviceKey` = service ปลายทาง)
2. service ปลายทางกันการทำซ้ำด้วย id ของคำสั่ง แล้ว **ส่งข้อมูลที่ตัวเองเป็นเจ้าของขึ้นไปใหม่** เช่น application ส่ง `sync-application` · unit ส่ง organization/role · service ธุรกิจส่งข้อมูล domain ของตัวเอง
3. ทุก service ส่ง `sync-permission` ของตัวเองให้ access-control
4. service ล้าง cache Redis ของตัวเอง

สั่งเองได้ที่ **system-admin › app › refresh-data** (เลือกทุก service หรือบางตัว) เช่นหลังแก้เมนูของหน้า admin ย่อย หรือเมื่อต้องการให้สำเนาข้อมูลตรงกันใหม่

---

<br>

<a id="keys"></a>

## กุญแจ 3 ชั้น (ภาพรวม)

| ชั้น | ผูกกับ | เกิดเมื่อ | ใช้ทำอะไร |
|---|---|---|---|
| **SystemCertificate** | service หนึ่งตัว | register service (core set: ตอน init-system) | ยืนยันตัวตนของ service ต่อ core · ให้ core ส่งข้อมูลที่มีแต่ service นั้นถอดได้ (เช่น privateKey ของ AppCertificate) |
| **AppCertificate** | service หนึ่งตัวในแอปหนึ่งแอป | เพิ่ม service เข้าแอป | publicKey/privateKey สำหรับเข้ารหัสข้อมูลถึง service นั้นในแอปนั้นเท่านั้น · เป็นตารางเดียวที่บอกว่า service ไหนมีสิทธิ์ในแอปไหน |
| **AppCredential** | แอป (ต่อ `clientId`) | สร้าง credential ของแอปใน authentication | ตรวจ token ของผู้ใช้ในแอป (key ตรวจ JWT + กฎของ token) · credential type `SYSTEM` ให้ระบบภายนอกเรียก API ด้วย `clientId` + `clientSecret` |

ทั้ง AppCertificate และ AppCredential ถูก sync มาไว้ที่ทุก service ล่วงหน้า ⇒ service ตรวจ token และสิทธิ์ของ request ได้เองโดยไม่ต้องถามใคร

---

<br>

<a id="plug-in"></a>

## ลำดับการเสียบ service ใหม่

1. สร้าง service จาก template แล้วตั้ง `serviceKey` ของตัวเอง (ดู [สร้าง Service ใหม่](createService.md) · [สิ่งที่ Service ต้องมี](serviceX.md))
2. register service ใน system-admin (`isCoreSet: false`) แล้วเก็บกุญแจที่ได้ไว้กับ service
3. เพิ่ม service เข้าแอปที่ต้องใช้ ⇒ service ได้ AppCertificate · AppCredential · ข้อมูลแอป · UserPolicy ⇒ ตรวจ request จากหน้าบ้านได้ทันที
4. สั่ง `refresh-data` ⇒ service ส่ง `sync-permission` ให้ access-control แล้วผู้ดูแลผูก permission เข้ากับ role
5. ต่องาน domain: เพิ่ม topic ที่ต้องรับ/ส่ง · เก็บสำเนาข้อมูลของ service อื่นไว้ใช้เอง
6. ใช้บริการกลางผ่าน topic (ตั้งเวลา · แจ้งเตือน · ไฟล์) — ไม่ทำ cron เอง ให้ใช้ schedule
7. ถ้ามีหน้า admin ย่อย: ทำตาม [หน้าบ้าน (Admin UI)](frontends.md#menu-metadata)

---

> อัปเดตจากคำผู้พัฒนาและโค้ด gumon-tech · 2026-10-05
