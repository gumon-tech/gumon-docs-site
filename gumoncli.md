# Gumon CLI

`gumon` เป็นเครื่องมือบรรทัดคำสั่งสำหรับ **ตั้งและรันระบบ Gumon บนเครื่องนักพัฒนา** ด้วยคำสั่งเดียว — ขึ้นฐานข้อมูล ตั้งระบบครั้งแรก (init) และขึ้น core set ให้พร้อมใช้ โดย **ไม่ต้อง init ใหม่ทุกครั้งที่เปิดเครื่อง**

CLI ตัวนี้เขียนใหม่ทั้งหมดในปี 2026 (TypeScript บน Node) มาแทน CLI รุ่นเดิมที่มีแค่ `gumon init` และแทนชุด docker compose ที่เคยแยกเป็นสองรีโป

!!! note "สถานะ"
    ใช้ได้แล้ว 11 คำสั่ง — ดูตาราง "คำสั่งทั้งหมด" ข้างล่าง · `gumon --help` บอกสถานะล่าสุดของทุกคำสั่งเสมอ (ตัวที่มี `·` นำหน้า = ยังไม่ทำ)

---

## สิ่งที่ต้องมี

- Docker พร้อม `docker compose` รุ่น 2
- Node.js 20 ขึ้นไป และ `pnpm`
- สิทธิ์ดึง image ของ core set จาก registry ของทีม (login ด้วย `docker login` ไว้ก่อน)
- RAM ว่างราว 4 GB สำหรับทั้งชุด (ฐานข้อมูล + core set 9 service + หน้า system admin)

## ติดตั้ง

source อยู่ในรีโป `gumon-cli`

```bash
git clone <รีโป gumon-cli>
cd gumon-cli
pnpm install
pnpm run check            # build + ทดสอบ
node dist/cli.js --help   # หรือ pnpm link --global แล้วเรียก gumon ได้ทุกที่
```

ตัวอย่างในหน้านี้เขียนเป็น `gumon …` — ถ้ายังไม่ได้ link ให้แทนด้วย `node <ที่อยู่ gumon-cli>/dist/cli.js …`

---

## ใช้งานประจำวัน

```bash
mkdir my-gumon && cd my-gumon   # โฟลเดอร์ทำงาน — สถานะของ stack เก็บที่นี่
gumon up                        # ขึ้นทั้งชุด
gumon status                    # ดูว่าอะไรพร้อมแล้ว
gumon down                      # หยุด (ข้อมูลยังอยู่)
```

สั่ง `gumon up` จากโฟลเดอร์เดิมทุกครั้ง — ทั้งเครื่องมี stack ได้ชุดเดียว

### up

`gumon up` ทำตามลำดับ และหยุดพร้อมบอกเหตุถ้าขั้นใดไม่ผ่าน

1. ขึ้นฐานข้อมูล: MongoDB (replica set), Kafka, Redis และที่เก็บไฟล์แบบ S3 — แล้วรอจนทุกตัวพร้อม
2. ตรวจว่า **ระบบนี้เคย init แล้วหรือยัง** (ดูทั้งไฟล์กุญแจของ core และข้อมูลในฐาน ไม่เชื่อฝั่งเดียว)
    - ยังไม่เคย → ให้ Core Service รัน init-system หนึ่งครั้ง (ดู [วงจรของ Service](serviceLifecycle.md))
    - เคยแล้ว → ข้ามขั้นนี้
3. ขึ้น core set ทุกตัวและหน้า system admin แล้วรอจนตอบได้

ครั้งแรกจากศูนย์ใช้เวลาราว 2–3 นาที (ไม่รวมเวลาดึง image) · ครั้งถัดไปไม่ถึงนาที เพราะไม่ init ซ้ำ

!!! warning "ยังต้องทำเองหลัง `up` ครั้งแรก"
    `up` ยังไม่สั่ง refresh-data ให้ — permission ของแต่ละ service จะยังไม่ถึง Access Control จนกว่าจะสั่ง refresh-data ของแต่ละ service จากหน้า system admin (เมนู refresh data) หรือ GraphQL ของ [Core Service](coreService.md) · คำสั่ง `gumon refresh-all` ที่จะทำขั้นนี้ให้อยู่ในแผน

### status

แสดงสถานะของฐานข้อมูลแต่ละตัว, ระบบ init แล้วหรือยัง และ service ใดตอบได้แล้ว

### down

หยุดและลบ container ทั้งชุด **โดยเก็บข้อมูลไว้ครบ** (ฐานข้อมูล, ข้อความใน Kafka, ไฟล์, กุญแจ) — `gumon up` รอบถัดไปกลับมาที่สถานะเดิม

### ตัวเลือกรวม

| ตัวเลือก | คำอธิบาย |
| --- | --- |
| `--help` | รายการคำสั่งพร้อมสถานะ |
| `--version` | รุ่นของ CLI |
| `--json` | ผลลัพธ์เป็น JSON สำหรับสคริปต์และ AI |

---

## โฟลเดอร์ `.gumon-stack/`

CLI สร้างโฟลเดอร์นี้ในโฟลเดอร์ทำงานตอน `up` ครั้งแรก

| ที่ | คำอธิบาย |
| --- | --- |
| `.gumon/login.txt` | ข้อมูลเข้าระบบของผู้ดูแลระบบคนแรก — ใช้เข้าหน้า system admin · เก็บเป็นความลับ |
| `.gumon/certificates/<serviceKey>/` | กุญแจของแต่ละ service ที่ Core Service ออกให้ตอน init |
| `infra.env` | รหัสผ่านของฐานข้อมูลที่ CLI สุ่มให้ครั้งแรก |

ห้าม commit โฟลเดอร์นี้ลง git และห้ามส่งต่อให้ผู้อื่น — ลบโฟลเดอร์นี้โดยไม่ล้างฐานข้อมูลจะทำให้กุญแจกับข้อมูลไม่ตรงกัน

---

## คำสั่งทั้งหมด

| คำสั่ง | ทำอะไร | สถานะ |
| --- | --- | --- |
| `gumon up` | ขึ้นฐานข้อมูล → init เฉพาะถ้ายังไม่เคย → ขึ้น core set · `--only a,b` ขึ้นเฉพาะที่ระบุ | ใช้ได้ |
| `gumon status` | สถานะของทั้งชุด และ RAM ที่แต่ละตัวใช้ | ใช้ได้ |
| `gumon down` | หยุด เก็บข้อมูลครบ | ใช้ได้ |
| `gumon snapshot [ชื่อ]` | เก็บสถานะปัจจุบันทั้งชุดเป็นไฟล์เดียว | ใช้ได้ |
| `gumon restore <ชื่อ>` | กลับสู่สถานะที่เก็บไว้โดยไม่ต้อง init ใหม่ (ต้องยืนยัน) | ใช้ได้ |
| `gumon reset` | ล้างทั้งหมดเพื่อเริ่มจากศูนย์ (ต้องยืนยัน) | ใช้ได้ |
| `gumon dev <service> [path]` | สลับ service ตัวเดียวไปรันจากโค้ดในเครื่อง ตัวอื่นยังรันจาก image · `--stop` สลับกลับ | ใช้ได้ |
| `gumon enable` / `disable <service>` | เปิด/ปิด service ที่งานนี้ไม่ใช้ เพื่อประหยัด RAM (จำค่า) | ใช้ได้ |
| `gumon update [service…]` | ดึง image รุ่นใหม่แล้วสร้าง container ใหม่ · `up` บอกให้เองว่าตัวใดล้าหลัง | ใช้ได้ |
| `gumon service register <path>` | ลงทะเบียน service ใหม่กับ Core Service วางกุญแจ และผูกเข้าแอประบบ | ใช้ได้ |
| `gumon refresh-all` | สั่ง refresh-data ทุกแอปและทุก service | อยู่ในแผน |
| `gumon logs` · `gumon doctor` | ดู log · ตรวจ docker, พอร์ต และ env ที่ขาด | อยู่ในแผน |
| `gumon app create` | ขึ้นแอปใหม่ครบขั้นในคำสั่งเดียว | อยู่ในแผน |

งานที่ยังไม่มีคำสั่ง เช่น เพิ่ม service เข้าแอปอื่นหรือถอด service ออก ทำผ่านหน้า system admin หรือ GraphQL ของ [Core Service](coreService.md) (`registerService`, `addServiceToApp`, `removeServiceFromApp`, `resignService`) ดูขั้นตอนที่ [สร้าง Service ใหม่](createService.md)

---

## เมื่อมีปัญหา

| อาการ | ทำอย่างไร |
| --- | --- |
| `up` หยุดที่ขั้นฐานข้อมูล | ดู `gumon status` ว่าตัวใดไม่พร้อม · ตรวจว่าไม่มีโปรแกรมอื่นใช้พอร์ตของ MongoDB, Kafka หรือ Redis อยู่ |
| ดึง image ไม่ได้ | ตรวจ `docker login` กับ registry ของทีม |
| `up` แจ้งว่า init ค้างครึ่งทาง (มีกุญแจแต่ฐานว่าง หรือกลับกัน) | CLI จะไม่ init ทับให้เอง — ล้างแล้วเริ่มใหม่ด้วย `gumon reset` (ข้อมูลใน stack หายทั้งหมด · เก็บ `gumon snapshot` ไว้ก่อนถ้ายังต้องใช้) |
| เข้าหน้า system admin ไม่ได้ | ใช้ข้อมูลใน `.gumon-stack/.gumon/login.txt` |

---

> อัปเดตจากโค้ด gumon-cli@bc19acf · 2026-10-10
