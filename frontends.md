# หน้าบ้าน (Admin UI)

หน้าบ้านอยู่ใน Application Layer — **เรียก GraphQL API ของแต่ละ service ตรง** (ต่างจาก service ↔ service ที่ต้องผ่าน Kafka · ดู [แนวคิด Gumon](concept.md))
หน้า admin ของ Gumon แบ่งเป็น 3 ชนิด

- [3 ชนิดของหน้า admin](#types)
- [dynamic-admin ทำงานอย่างไร](#dynamic-admin)
- [เมนูของ admin ย่อยเข้าระบบได้อย่างไร](#menu-metadata)

---

<br>

<a id="types"></a>

## 3 ชนิดของหน้า admin

- **system admin** คือที่ที่ผู้ดูแลระบบตั้งค่าไว้ก่อน (แอป · service · permission · เมนูกลาง) แล้วส่งต่อให้ผู้ใช้จริง
- **dynamic-admin** คือที่ที่ผู้ใช้จริงของแต่ละแอปตั้งค่าต่อเอง (ผูก permission เข้า role · user · องค์กร) ทั้งระดับแอปและระดับองค์กร
- **หลักการ: 1 API ธุรกิจ = 1 หน้า admin ย่อยประกบเสมอ** ฝังใน dynamic-admin ให้ขึ้นงานได้เร็วด้วยของกลาง · ทำ **admin แยก** เฉพาะเมื่อลูกค้าต้องการหน้าของตัวเองที่มี flow เฉพาะมาก
- **ทางที่นิยมตอนนี้:** เริ่มจาก `gumon-admin-template` (มี AGENTS.md และ skill สำหรับให้ AI สร้างหน้าจาก GraphQL) พร้อมไฟล์ `.gql` ของ service ที่ใช้ — ขึ้นหน้า admin ได้เร็วกว่าการทำ admin ย่อยแบบ iframe

| ชนิด | repo | ใช้ทำอะไร |
|---|---|---|
| **system admin** — ตั้งค่าทั้งระบบ | `gumon-system-admin` | สร้างแอป · Custom Menu · Permission · Service (register / resign) · Service hand-check · Service ready · service settings · system certificate · Theme · HostTheme · และงานระดับแอป เช่น เพิ่ม/ถอด service ในแอป · credential · refresh-data · role · user |
| **dynamic-admin** — ศูนย์กลางของแต่ละแอป | `gumon-dynamic-admin` | ผู้ดูแลของแอปเข้ามาจัดการ user / role / organization แล้วเปิดหน้า admin ย่อยของแต่ละ service ได้จากที่เดียว ตามเมนูที่ role ของตัวเองเห็น |
| **admin เฉพาะงาน** — ตรงงาน 100% | `gumon-admin-template` (แบบ standalone) · admin ย่อยจาก `gumon-dynamic-admin-iframe-template` (เช่น `gumon-project-management-admin`) | standalone: แอป admin แยกของตัวเอง login เอง · admin ย่อย: หน้าจอของ service หนึ่งตัวที่ถูกโหลดเข้าไปอยู่ใน dynamic-admin |

ทุกตัวใช้ Next.js (App Router) + Ant Design + Apollo Client · ส่ง header `Authorization` (token ผู้ใช้ ซึ่งมี clientId และ appKey อยู่ในตัวแล้ว) ไปหา service · `X-APP-CLIENT-ID` ใช้เฉพาะตอนยังไม่มี token เช่น หน้า login หรือหน้าสาธารณะ · admin ย่อยจึงส่งแค่ `Authorization` ได้

---

<br>

<a id="dynamic-admin"></a>

## dynamic-admin ทำงานอย่างไร

1. เปิดเว็บที่โดเมนของแอป → dynamic-admin หาแอปจาก hostname ด้วย **HostTheme** ของ application (ได้ appKey, clientId, ธีม) ⇒ deployment เดียวรับได้หลายแอปตามโดเมน
2. ผู้ใช้ login ผ่าน authentication
3. ดึงเมนูของผู้ใช้จาก access-control (`getMyCustomMenus` ระดับแอปหรือระดับองค์กร) — เมนูถูกกรองตาม role ของผู้ใช้แล้ว
4. แสดงหน้าตามชนิดของเมนู (`CustomMenu.type`)

| type | พฤติกรรม |
|---|---|
| `INTERNAL` | หน้าที่อยู่ในตัว dynamic-admin เอง (เช่น จัดการ user / role / organization) |
| `IFRAME` | โหลดหน้า admin ย่อยของ service ผ่าน iframe — **วิธีที่ใช้อยู่ในปัจจุบัน** |
| `MICRO_FRONTEND` | โหลดหน้า admin ย่อยแบบ micro frontend — **อยู่ในแผน** |
| `EXTERNAL_LINK` · `EXTERNAL_DOWNLOAD` | ลิงก์ออกนอกระบบ / ดาวน์โหลด — อยู่ในแผน |

### admin ย่อยแบบ iframe

dynamic-admin เป็นผู้ถือ session ของผู้ใช้ แล้วส่งข้อมูลที่ admin ย่อยต้องใช้ให้ผ่าน `postMessage`

| ทิศ | ข้อความ | เนื้อหา |
|---|---|---|
| dynamic-admin → admin ย่อย | `'are-you-ready'` | ถามว่าพร้อมรับข้อมูลหรือยัง |
| admin ย่อย → dynamic-admin | `'iframe-ready'` | พร้อมรับข้อมูล |
| dynamic-admin → admin ย่อย | `{type:'auth:update', ...}` | `accessToken` · `themeData` (appKey, clientId, theme) · `locale` · `isDarkMode` · `paramData` (query params ของเมนู) · `orgKey` |
| admin ย่อย → dynamic-admin | `{type:'resize', height}` | ปรับความสูงของ iframe |
| admin ย่อย → dynamic-admin | `{type:'path-change', path}` | แจ้ง path ปัจจุบัน |
| admin ย่อย → dynamic-admin | `{type:'loading-status', loading}` | แจ้งสถานะกำลังโหลด |

- dynamic-admin เป็นผู้ต่ออายุ token แล้วส่ง `auth:update` ใหม่ให้ admin ย่อยเอง — admin ย่อยใช้แค่ accessToken เรียก GraphQL ของ service
- เริ่มทำ admin ย่อยจาก `gumon-dynamic-admin-iframe-template` ซึ่งรับส่งข้อความชุดนี้ไว้ให้แล้ว
- dynamic-admin มีหน้า dev tool ไว้ทดสอบ admin ย่อยที่รันบนเครื่องตัวเอง (localhost) โดยไม่ต้องลงทะเบียนเมนู

---

<br>

<a id="menu-metadata"></a>

## เมนูของ admin ย่อยเข้าระบบได้อย่างไร

1. admin ย่อยเปิด endpoint **menu metadata** `GET /api/menuMetaData` คืนรายการ `CustomMenu[]` ของตัวเอง (ชื่อเมนู · type · url · query params)
2. ตอน register service ใน system-admin กรอก `urlFrontend` (ที่อยู่ของ admin ย่อย) และ `urlGetMetaData` (ที่อยู่ของ endpoint ข้อ 1)
3. core ส่ง `sync-service` ให้ access-control
4. access-control เรียก `urlGetMetaData` แล้วบันทึกเมนูของ service นั้น
5. แก้เมนูภายหลัง → สั่ง `refresh-data` ให้ access-control ดึงเมนูใหม่
6. ผู้ดูแลแอปผูกเมนูเข้ากับ role ใน dynamic-admin › user management ⇒ ผู้ใช้ที่มี role นั้นเห็นเมนูใน dynamic-admin

---

> อัปเดตจากคำผู้พัฒนาและโค้ด gumon-tech · 2026-10-05
