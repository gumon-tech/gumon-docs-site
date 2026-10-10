# Storage Service

Service กลางสำหรับจัดการไฟล์ของทุกแอป · serviceKey: `storage`

storage ไม่รับตัวไฟล์ผ่านตัวเองตอนอัปโหลด แต่ออก **presigned URL** ให้หน้าบ้านอัปโหลด / ดาวน์โหลดกับ object storage (S3 หรือ S3-compatible) โดยตรง แล้วเก็บข้อมูลไฟล์ (metadata) ไว้อ้างอิงด้วย `fileKey`

<br>

- [ภาพรวมการใช้งาน](#ภาพรวมการใชงาน)
- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [kafka produce Reference](#kafka-produce-reference)

---

<br>
<br>

## ภาพรวมการใช้งาน

### อัปโหลดไฟล์

1. หน้าบ้านเรียก [Put Signed Upload File](#put-signed-upload-file) (login แล้ว) หรือ [Put Public Signed Upload File](#put-public-signed-upload-file) (ยังไม่ login) พร้อม `fileName`, `contentType`, `path`, `acl`
2. ได้ `signedUrl` (อายุ 1 ชั่วโมง) และ `fileKey` กลับมา
3. หน้าบ้าน `PUT` ตัวไฟล์ไปที่ `signedUrl` โดยตรง ใส่ header `Content-Type` ให้ตรงกับ `contentType` ที่ขอไว้

   ```js
   await fetch(signedUrl, { method: "PUT", headers: { "Content-Type": file.type }, body: file });
   ```

4. เก็บ `fileKey` ไว้ในข้อมูลของ service ตัวเอง (เช่น รูปโปรไฟล์ เอกสารแนบ) ใช้อ้างถึงไฟล์นี้ต่อไป
5. storage ตั้งเวลากับ [Schedule Service](scheduleService.md) ไว้ตอนหมดอายุ signedUrl แล้วตรวจว่าไฟล์ถูกอัปโหลดจริงหรือไม่ · สถานะไฟล์จะเปลี่ยนจาก `UNKNOWN` เป็น `SUCCESS` (พร้อมขนาดไฟล์) หรือ `FAILED`

### File key

รูปแบบ `fileKey` = `<appKey>/<path>/<id><นามสกุลไฟล์>` เช่น `myApp/profile/6650f0c2a1b2c3d4e5f60718.png`

- `path` คือหมวดที่ใช้จัดกลุ่มไฟล์ในแอป (ค่าเริ่มต้น `default`)
- ไฟล์ทุกไฟล์อยู่ใต้ `appKey` ของแอปตัวเอง

### ดาวน์โหลด / แสดงไฟล์

- `acl: PUBLIC` ใช้ `publicUrl` ได้ตรง ๆ
- `acl: PRIVATE` ต้องเรียก [Get Signed Url](#get-signed-url) ด้วย `fileKey` เพื่อขอ URL ชั่วคราว (อายุ 1 ชั่วโมง) ทุกครั้งที่จะแสดงไฟล์ · ต้อง login
- `publicUrl` อาจชี้ไปที่ object storage โดยตรง หรือชี้มาที่ `GET /file/<fileKey>` ของ storage service (ส่งไฟล์แบบ stream, `Content-Disposition: inline`) ขึ้นกับการตั้งค่าของระบบ ให้ใช้ URL ตามที่ได้รับ

### ไฟล์ที่ service อื่นสร้างเอง

service ที่สร้างไฟล์ลง object storage เอง (เช่น ผลการแปลงวิดีโอ) ลงทะเบียนไฟล์นั้นกับ storage ผ่าน Kafka topic [`sync-file-upload`](#sync-file-upload) เพื่อให้เรียกดูผ่าน API เดียวกันได้

---

## API Reference
---

GraphQL endpoint `/graphql` · API ที่ต้อง login ใช้ header `authorization` และต้องมี permission ตามที่ระบุ · API ที่ระบุว่า "app credential" เรียกได้ทั้งแบบ login หรือแบบไม่ login โดยใช้ client ของแอป (header `X-APP-CLIENT-ID` และสำหรับระบบภายนอกเพิ่ม `X-APP-CLIENT-SECRET`)

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

#### Get Signed Url

ขอ URL สำหรับเปิดไฟล์จาก `fileKey` (ได้หลายไฟล์พร้อมกัน) · app credential

- ไฟล์ `PUBLIC` คืน `publicUrl`
- ไฟล์ `PRIVATE` คืน `signedUrl` อายุ 1 ชั่วโมง และ `expired` · ต้อง login ถ้าไม่ login จะได้ error Forbidden
- ทุก `fileKey` ต้องมีอยู่จริงในแอป ไม่อย่างนั้นได้ error Not Found

```graphql
query {
  getSignedUrl(fileKeys: ["myApp/profile/6650f0c2a1b2c3d4e5f60718.png"]) {
    _id fileKey fileName acl publicUrl signedUrl expired contentType
  }
}
```

Response : `[FileUploadsUrl]`

FileUploadsUrl

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| acl | ENUM | `PUBLIC`, `PRIVATE` |
| signedUrl | String | URL ชั่วคราว (อัปโหลด: ใช้ `PUT` ไฟล์ · ดาวน์โหลดไฟล์ PRIVATE: ใช้เปิดไฟล์) |
| publicUrl | String | URL ของไฟล์ (ไฟล์ PRIVATE ต้องใช้ signedUrl แทน) |
| fileKey | String | key อ้างอิงไฟล์ |
| fileName | String | ชื่อไฟล์ |
| contentType | String | MIME type |
| expired | Date | เวลาหมดอายุของ signedUrl |
| fileType | ENUM | `INTERNAL` (อัปโหลดผ่าน storage) · `EXTERNAL` (ลงทะเบียนจากภายนอก) |

---

#### Get File Uploads

ดึงรายการไฟล์ของแอปที่ login (แบ่งหน้า) · permission: `getFileUploads`

```graphql
query {
  getFileUploads(input: {
    filter: { acl: PRIVATE, path: "myApp/profile" }
    search: { fileName: "invoice" }
    sort: { sortBy: createdAt, sortOrder: DESC }
    pagination: { limit: 20, page: 1 }
  }) {
    fileUploads { _id fileKey fileName contentType acl authId createdAt }
    pagination { totalItems totalPages hasNextPage }
  }
}
```

Input `GetFileUploadsInput`

| key | Type | คำอธิบาย |
| --- | --- | --- |
| filter | GetFileUploadsFilterInput | `acl`, `signedUrl`, `publicUrl`, `path`, `fileKey`, `fileName`, `contentType`, `authId`, `fileType` |
| search | GetFileUploadsSearchInput | ค้นหาบางส่วน: `path`, `fileKey`, `fileName`, `contentType` |
| sort | GetFileUploadsSortInput | `sortBy` (ค่าเริ่มต้น `createdAt`), `sortOrder` ASC / DESC |
| pagination | CustomPaginateInput | `limit` (ค่าเริ่มต้น 10), `page` (ค่าเริ่มต้น 1) |

Response : `FileUploadsPagination` = `{ fileUploads: [FileUploads], pagination: CustomPaginate }`

FileUploads

| key | Type | คำอธิบาย |
| --- | --- | --- |
| _id | ID | id ที่ใช้อ้างอิง |
| acl | ENUM | `PUBLIC`, `PRIVATE` |
| signedUrl | String | URL ที่ใช้อัปโหลดตอนสร้าง |
| publicUrl | String | URL ของไฟล์ |
| path | String | หมวดของไฟล์ (รวม appKey นำหน้า) |
| fileKey | String | key อ้างอิงไฟล์ |
| fileName | String | ชื่อไฟล์ |
| contentType | String | MIME type |
| authId | String | user ที่อัปโหลด |
| expired | Date | เวลาหมดอายุของ signedUrl ตอนอัปโหลด |
| fileType | ENUM | `INTERNAL`, `EXTERNAL` |
| createdBy, updatedBy | String | ผู้สร้าง / ผู้แก้ไข |
| createdAt, updatedAt | Date | เวลาสร้าง / แก้ไขล่าสุด |

---

#### Get File Upload By ID

ดึงข้อมูลไฟล์ตาม `_id` · permission: `getFileUploadById`

```graphql
query {
  getFileUploadById(fileUploadId: "6650f0c2a1b2c3d4e5f60718") { _id fileKey fileName acl publicUrl }
}
```

Response : `FileUploads`

---

#### Get File Upload By File Key

ดึงข้อมูลไฟล์ตาม `fileKey` · permission: `getFileUploadByFileKey`

```graphql
query {
  getFileUploadByFileKey(fileKey: "myApp/profile/6650f0c2a1b2c3d4e5f60718.png") { _id fileName acl publicUrl }
}
```

Response : `FileUploads`

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

#### Put Signed Upload File

ขอ presigned URL สำหรับอัปโหลดไฟล์ (อายุ 1 ชั่วโมง) แล้วบันทึกข้อมูลไฟล์สถานะ `UNKNOWN` และตั้งเวลาตรวจผลการอัปโหลดกับ schedule · ต้อง login · permission: `putSignedUploadFille`

```graphql
mutation {
  putSignedUploadFile(input: {
    acl: PRIVATE
    path: "documents"
    fileName: "invoice-2026-10.pdf"
    contentType: "application/pdf"
  }) {
    _id fileKey signedUrl publicUrl expired
  }
}
```

Input `CreateFileUploadsInput`

| key | Type | คำอธิบาย |
| --- | --- | --- |
| acl | ENUM | `PUBLIC` (ค่าเริ่มต้น) หรือ `PRIVATE` |
| path | String | หมวดของไฟล์ (ค่าเริ่มต้น `default`) |
| fileName | String! | ชื่อไฟล์พร้อมนามสกุล (นามสกุลใช้ต่อท้าย fileKey) |
| contentType | String! | MIME type ของไฟล์ ต้องตรงกับ header `Content-Type` ตอน `PUT` |

Response : `FileUploadsUrl`

---

#### Put Public Signed Upload File

ขอ presigned URL สำหรับอัปโหลดไฟล์โดยไม่ต้อง login (เช่น ฟอร์มสาธารณะ) · app credential · ไฟล์เป็น `acl: PUBLIC` เสมอ และบันทึกผู้อัปโหลดเป็น `SYSTEM` · ชื่อ API สะกด `Singed` ตามโค้ด

```graphql
mutation {
  putPublicSingedUploadFile(input: {
    path: "public-form"
    fileName: "photo.jpg"
    contentType: "image/jpeg"
  }) {
    _id fileKey signedUrl publicUrl expired
  }
}
```

Input `CreatePublicFileUploadsInput`: `path` (ค่าเริ่มต้น `default`), `fileName: String!`, `contentType: String!`

Response : `FileUploadsUrl`

---

#### Delete File Upload

ลบข้อมูลไฟล์และลบไฟล์ใน object storage ตาม `fileKey` (ได้หลายไฟล์) · permission: `deleteFileUpload`

```graphql
mutation {
  deleteFileUpload(fileKeys: ["myApp/documents/6650f0c2a1b2c3d4e5f60718.pdf"]) { _id fileKey }
}
```

Response : `[FileUploads]` (ข้อมูลที่ถูกลบ)

---

### REST

#### Get File

    GET /file/<fileKey>

ส่งไฟล์แบบ stream ผ่าน storage service (`Content-Disposition: inline` พร้อม `Content-Type` ของไฟล์) · ใช้เป็น `publicUrl` เมื่อระบบตั้งค่าให้เปิดไฟล์ผ่าน storage · ไม่พบไฟล์ได้ HTTP 404

---

## kafka consume Reference

topic ส่วนใหญ่ตรวจว่า storage มีสิทธิ์ในแอปตาม `appKey` (มี appCertificate ของแอปนั้น) ก่อนทำงาน

### Standard topics

| topic | คำอธิบาย |
| --- | --- |
| `init-system` | ตั้งระบบจากศูนย์ (เฉพาะ core set) |
| `refresh-data` | ส่งข้อมูลที่ตัวเองถือขึ้นไปใหม่ (เช่น permission) และล้าง cache |
| `sync-app-certificate` | รับ appCertificate ของแอปที่ storage มีสิทธิ์ |
| `sync-app-credential` | รับข้อมูลการเข้าใช้ของแอป (ใช้ตรวจ token และ app credential) |
| `sync-application` | รับข้อมูลแอป |
| `sync-user-policy` | รับ UserPolicy สำหรับตรวจ permission |
| `sync-service-setting` | รับค่าตั้งค่าเพิ่มเติมของ storage ระดับระบบ (header `serviceKey: storage`) |
| `sync-app-service-setting` | รับค่าตั้งค่าเพิ่มเติมของ storage เฉพาะแอป (header `serviceKey: storage`) |

---

### Sync File Upload

ลงทะเบียนไฟล์ที่ service อื่นสร้างไว้ใน object storage แล้ว ให้ storage รู้จักและเรียกดูผ่าน API ได้

    topic: sync-file-upload
    header: appKey, serviceKey = storage
    Action: ADD

```json
{
  "action": "ADD",
  "fileUpload": {
    "appKey": "myApp",
    "acl": "PRIVATE",
    "publicUrl": null,
    "path": "myApp/videos",
    "fileKey": "myApp/videos/6650f0c2a1b2c3d4e5f60718/index.m3u8",
    "fileName": "index.m3u8",
    "fileSize": 1024,
    "fileType": "EXTERNAL",
    "contentType": "application/vnd.apple.mpegurl",
    "authId": "665000000000000000000030",
    "status": "SUCCESS",
    "createdBy": "665000000000000000000030",
    "updatedBy": "665000000000000000000030"
  }
}
```

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | string | แอปเจ้าของไฟล์ |
| acl | ENUM | `PUBLIC`, `PRIVATE` |
| publicUrl | string \| null | URL ของไฟล์ (ถ้ามี) |
| path | string | หมวดของไฟล์ |
| fileKey | string | key ของไฟล์ใน object storage |
| fileName | string | ชื่อไฟล์ |
| fileSize | number \| null | ขนาดไฟล์ (byte) |
| fileType | string | รูปแบบการจัดการไฟล์ เช่น `EXTERNAL` |
| contentType | string | MIME type |
| authId | string | user เจ้าของไฟล์ |
| status | ENUM | `UNKNOWN`, `SUCCESS`, `FAILED` (ค่าเริ่มต้น `SUCCESS`) |
| createdBy, updatedBy | string | ผู้สร้าง / ผู้แก้ไข |

---

### Schedule Alarm

รับ alarm จาก [Schedule Service](scheduleService.md#schedule-alarm) เมื่อ signedUrl ของการอัปโหลดหมดอายุ · รับเฉพาะรายการที่ `scheduleAlarm.serviceKey = storage` แล้วใช้ `scheduleRefKey` (= `_id` ของไฟล์) ตรวจกับ object storage ว่ามีไฟล์จริงหรือไม่ จากนั้นอัปเดตสถานะเป็น `SUCCESS` (พร้อมขนาดไฟล์) หรือ `FAILED`

    topic: schedule-alarm
    Action: ADD

| key | Type | คำอธิบาย |
| --- | --- | --- |
| scheduleAlarm.serviceKey | string | `storage` |
| scheduleAlarm.scheduleRefKey | string | `_id` ของไฟล์ |
| scheduleAlarm.scheduleRefType | string | `fileUploads` |
| scheduleAlarm.alarmData | object | `_id`, `description`, `timeStamp`, `expiredTime` |

---

## Kafka Produce Reference

### Set Schedule

ลงทะเบียนเวลาตรวจผลการอัปโหลดกับ schedule ทุกครั้งที่ออก presigned URL สำหรับอัปโหลด (ครั้งเดียว ไม่วนซ้ำ ที่เวลาหมดอายุของ signedUrl)

    topic: set-schedule
    header: appKey, serviceKey = schedule
    Action: ADD

| key | Type | คำอธิบาย |
| --- | --- | --- |
| schedule.appKey | string | แอปของไฟล์ |
| schedule.serviceKey | string | `storage` (ผู้รับ alarm) |
| schedule.scheduleRefKey | string | `_id` ของไฟล์ |
| schedule.scheduleRefType | string | `fileUploads` |
| schedule.alarmData | object | `_id`, `description`, `timeStamp`, `expiredTime` |
| schedule.title / description | string | `file upload` / `storage upload file: <fileName>` |
| schedule.isRecurring | boolean | `false` |
| schedule.notificationAt | Date | เวลาหมดอายุของ signedUrl |

---

### Sync Permission

ส่ง permission ของ storage ให้ access-control เมื่อได้รับ `refresh-data` เพื่อนำไปผูกกับ role (`putSignedUploadFille`, `getSignedUrl`, `getFileUploads`, `getFileUploadById`, `getFileUploadByFileKey`, `deleteFileUpload`)

    topic: sync-permission
    Action: ADD

---

> อัปเดตจากโค้ด gumon-storage-service@3f02969 · 2026-10-05
