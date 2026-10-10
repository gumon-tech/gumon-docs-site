# Gumon Docs

Gumon คือระบบแบบ **Composable System** — แอปหนึ่งตัวประกอบจาก service สำเร็จรูปหลายตัว
service คุยกันผ่าน **Kafka** เท่านั้น ส่วนหน้าบ้านเรียก GraphQL API ของแต่ละ service ได้ตรง

**จะต่อ service ใหม่เข้า Gumon ขอแค่คุยผ่าน Kafka ได้** — ภาษา ฐานข้อมูล framework เลือกเองได้

## เริ่มที่ไหน

| อยากทำ | อ่าน |
| --- | --- |
| เข้าใจภาพรวมก่อน | [แนวคิด Gumon](concept.md) |
| ทำ service ใหม่ต่อเข้าระบบ | [สิ่งที่ Service ต้องมี](serviceX.md) → [สร้าง Service ใหม่](createService.md) |
| หา topic และ payload | [Kafka topic มาตรฐาน](standardTopics.md) |
| ลงทะเบียน / เพิ่ม service เข้าแอป | [วงจรชีวิตของ service](serviceLifecycle.md) |
| ทำหน้า admin ฝังใน dynamic admin | [หน้าบ้าน (Admin UI)](frontends.md) |
| ดู API ของ service แต่ละตัว | เมนู **Core Service** ทางซ้าย — ต้นหัวข้อ API Reference ของทุกหน้ามีตารางสรุป |

## Core set

service ที่ระบบต้องมีเพื่อทำงานได้ก่อนมีธุรกิจใด ๆ

| service | หน้าที่ |
| --- | --- |
| [Core](coreService.md) | ทะเบียน service · กุญแจ · เพิ่ม service เข้าแอป |
| [Application](applicationService.md) | แอป · ธีม · โดเมน |
| [Authentication](authenticationService.md) | บัญชีผู้ใช้ · login · credential ของแอป |
| [ACL](aclService.md) | permission · ผูก role · UserPolicy |
| [Unit](unitService.md) | องค์กร · role · ประเภท/แท็ก · ผู้ติดต่อ · เลขรัน |
| [Profile](userService.md) | ข้อมูลส่วนตัวผู้ใช้ |
| [Notification](notificationService.md) | แจ้งเตือน in-app · email · SMS |
| [Schedule](scheduleService.md) | ตั้งเวลา / งานตามรอบ แทน cron ในแต่ละ service |
| [Storage](storageService.md) | อัปโหลด / ดาวน์โหลดไฟล์ |

## สำหรับ AI

ให้ AI อ่านเอกสารนี้ได้ทันทีที่ [`/llms.txt`](https://docs.gumon.io/llms.txt) (สารบัญ) และ [`/llms-full.txt`](https://docs.gumon.io/llms-full.txt) (ทุกหน้าในไฟล์เดียว)
