# Core Service

Core Service คือทะเบียนกลางของ service ทั้งระบบและเป็นผู้ออกกุญแจ ทำหน้าที่

- ลงทะเบียน / ถอด service ออกจากระบบ (ระดับระบบ) และเพิ่ม / ถอด service ออกจากแอป (ระดับแอป)
- ออก **System Certificate** (กุญแจระดับ service) และ **App Certificate** (กุญแจระดับ service × แอป)
- เก็บค่าตั้งค่าของ service ทั้งระบบ (service setting) และค่าตั้งค่าต่อแอป (app service setting)
- สั่ง `refresh-data` ให้ service ส่งข้อมูลที่ตัวเองถืออยู่ขึ้น Kafka ใหม่
- ตรวจว่า service ปลายทางถือกุญแจถูกต้อง (hand-check)
- ตั้งระบบจากศูนย์ (`init-system`) ครั้งแรก

serviceKey: `core` · เป็นส่วนหนึ่งของ core set

- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)
- ดูเพิ่ม: [สิ่งที่ Service ต้องมี](serviceX.md) · [สร้าง Service ใหม่](createService.md)

---

## แนวคิดสำคัญ

### core set

service ที่ระบบต้องมีเพื่อให้ทำงานได้โดยยังไม่มีธุรกิจเกี่ยว (`isCoreSet: true`):
`core` · `authentication` · `application` · `access-control` · `unit` · `profile` · `notification` · `schedule` · `storage`

service ธุรกิจที่ลงทะเบียนภายหลังเป็น `isCoreSet: false` (ค่า default ของ `registerService`)

### ถอดเข้าถอดออก 2 ระดับ

| ระดับ | เข้า | ออก | ผล |
| --- | --- | --- | --- |
| ระบบ | `registerService` | `resignService` | register ออก System Certificate ใหม่ให้ service · resign ลบข้อมูลของ service นั้น ไม่ส่งข้อมูลให้อีก ถ้าจะกลับมาต้อง register ใหม่ด้วยกุญแจใหม่ |
| แอป | `addServiceToApp` | `removeServiceFromApp` | จัดการ App Certificate ของ service ในแอปนั้น · ไม่มี App Certificate ในแอปไหน = รับข้อมูลของแอปนั้นไม่ได้ ใช้ข้ามแอปไม่ได้ |

แอปที่สร้างใหม่ (ผ่าน `sync-application`) จะถูกผูก service core set ทุกตัวเข้าให้อัตโนมัติ

### กุญแจ

- **System Certificate** — ออกตอน `registerService` · service เก็บไว้ใน `.gumon/certificates/<serviceKey>/` ของตัวเอง (ห้าม commit) · ใช้ถอดข้อมูลที่ core ส่งถึง service นั้นโดยเฉพาะ (hand-check, App Certificate)
- **App Certificate** — คู่กุญแจ RSA ต่อ (appKey, serviceKey) · core ส่งผ่าน `sync-app-certificate` โดย privateKey ถูกเข้ารหัสด้วย System Certificate ของ service เจ้าของ ⇒ มีแต่ service นั้นถอดได้ · ทุก service ใน app เก็บ publicKey ของกันและกัน ⇒ ตารางนี้ตารางเดียวบอกได้ว่า service ไหนมีสิทธิ์ในแอปไหน

---

## API Reference

endpoint: `POST /graphql`

ทุก operation ต้องส่ง header `Authorization: Bearer <accessToken>` และผู้เรียกต้องมี permission ชื่อเดียวกับ operation (serviceKey `core`) เว้นแต่ระบุไว้เป็นอย่างอื่น

query ที่เป็นรายการรับ input รูปเดียวกัน `{ filter, search, sort: { sortBy, sortOrder }, pagination }` และคืน `{ <รายการ>[], pagination }`

---

### Query

---

#### getServices

ดึงรายการ service ทั้งหมดในระบบ

```graphql
query {
  getServices(getServicesInput: { filter: { isCoreSet: false }, pagination: { page: 1, limit: 20 } }) {
    services { _id serviceKey name description version author isCoreSet isActive type urlFrontend urlGetMetaData createdAt }
    pagination { totalItems page limit }
  }
}
```

| field ของ Service | Type | คำอธิบาย |
| --- | --- | --- |
| serviceKey | String! | key ของ service (ไม่ซ้ำ) |
| name / description / version / author | String | ข้อมูลแสดงผล |
| isCoreSet | Boolean | เป็น core set หรือไม่ |
| isActive | Boolean | สถานะใช้งาน |
| type | `BACKEND` \| `MAIN_FRONTEND` \| `MICRO_FRONTEND` \| `OTHER` | ชนิดของ service |
| urlFrontend | String | URL หน้าบ้าน (กรณีเป็นหน้า admin ย่อย) |
| urlGetMetaData | String | URL ที่คืนเมนู (`CustomMenu[]`) ของหน้า admin ย่อย ให้ access-control ดึงไปสร้างเมนู |

permission: `getServices`

---

#### getServiceById

```graphql
query { getServiceById(serviceId: "<id>") { _id serviceKey name type } }
```

permission: `getServiceById`

---

#### getServiceByServiceKey

```graphql
query { getServiceByServiceKey(serviceKey: "storage") { _id serviceKey name isCoreSet } }
```

permission: `getServiceByServiceKey`

---

#### getServicesInApp

รายการ service ที่อยู่ในแอป (`AppService`: `appKey`, `serviceKey`)

```graphql
query {
  getServicesInApp(getInput: { filter: { appKey: "my-app" } }) {
    appServices { _id appKey serviceKey createdAt }
    pagination { totalItems }
  }
}
```

permission: `getServicesInApp`

---

#### getMyServicesInApp

รายการ service ในแอปของผู้ที่ login อยู่ (input เหมือน `getServicesInApp`) · ต้อง login

---

#### getServiceInAppById

```graphql
query { getServiceInAppById(id: "<id>") { appKey serviceKey } }
```

permission: `getServiceInAppById`

---

#### getSystemCertificates / getSystemCertificateById

ดูรายการ System Certificate ของแต่ละ service (`serviceId`, `serviceKey`, `createdAt`)

```graphql
query {
  getSystemCertificates(getSystemCertificatesInput: { filter: { serviceKey: "storage" } }) {
    systemCertificates { _id serviceId serviceKey createdAt }
  }
}
```

permission: `getSystemCertificates` · `getSystemCertificateById`

---

#### getAppCertificates / getAppCertificateById

ดู App Certificate (`appKey`, `serviceKey`, `publicKey`)

```graphql
query {
  getAppCertificates(input: { filter: { appKey: "my-app" } }) {
    appCertificates { _id appKey serviceKey publicKey }
  }
}
```

permission: `getAppCertificates` · `getAppCertificateById`

---

#### getServiceSettings / getServiceSettingByServiceKey

ค่าตั้งค่าทั้งระบบของ service (`setting`: JSON) — ใช้ตั้งค่าเพิ่มเติมผ่าน core แทนการแก้ env

```graphql
query { getServiceSettingByServiceKey(serviceKey: "notification") { serviceKey setting } }
```

permission: `getServiceSettings` · `getServiceSettingByServiceKey`

---

#### getAppServiceSettings / getAppServiceSetting / getAppServiceSettingById

ค่าตั้งค่าเฉพาะแอปของ service (`appKey`, `serviceKey`, `setting`: JSON)

```graphql
query { getAppServiceSetting(serviceKey: "authentication", appKey: "my-app") { appKey serviceKey setting } }
```

permission: `getAppServiceSettings` · `getAppServiceSetting` · `getAppServiceSettingById`

---

#### getServiceHandChecks / getServiceHandCheckById / getServiceHandCheckByHandCheckRefId

ประวัติการ hand-check (`handCheckRefId`, `systemCertificateId`, `serviceKey`, `start`, `end`, `isSuccess`)

```graphql
query {
  getServiceHandChecks(input: { filter: { serviceKey: "storage" } }) {
    serviceHandChecks { handCheckRefId serviceKey start end isSuccess }
  }
}
```

permission: `getServiceHandChecks` · `getServiceHandCheckById` · `getServiceHandCheckByHandCheckRefId`

---

#### getServiceReady / getServiceReadyById

สถานะความพร้อมของ service (`serviceKey`, `isHandCheck`, `lastHandCheckAt`)

```graphql
query { getServiceReady(input: {}) { serviceReady { serviceKey isHandCheck lastHandCheckAt } } }
```

permission: `getServiceReady` · `getServiceReadyById`

---

### Mutation

---

#### registerService

ลงทะเบียน service ใหม่เข้าระบบ (ระดับระบบ) และออก System Certificate ให้

```graphql
mutation {
  registerService(registerServiceInput: {
    name: "My Service"
    serviceKey: "my-service"
    description: "..."
    version: "1.0.0"
    type: BACKEND
    urlFrontend: null
    urlGetMetaData: null
  }) {
    service { _id serviceKey isCoreSet }
    systemCertificateId
    publicKey
    privateKey
    hashKey
    symmetricKey
  }
}
```

| input | Type | คำอธิบาย |
| --- | --- | --- |
| name | String! | ชื่อ service |
| serviceKey | String! | key ของ service ห้ามซ้ำ |
| description / version / author | String | |
| isCoreSet | Boolean = false | service ธุรกิจใช้ false |
| isActive | Boolean = true | |
| type | EnumServiceType = BACKEND | |
| urlFrontend / urlGetMetaData | String | สำหรับหน้า admin ย่อย |

ผลลัพธ์มีกุญแจที่ service ต้องเก็บไว้ใน `.gumon/certificates/<serviceKey>/` ของตัวเอง (ได้ครั้งเดียว เก็บให้ดี ห้าม commit):

| field | ไฟล์ |
| --- | --- |
| systemCertificateId | `certificate-id.key` |
| privateKey | `certificate` |
| publicKey | `certificate.pub` |
| hashKey | `hash.key` |
| symmetricKey | `symmetric.key` |

หลังลงทะเบียน core จะส่ง `sync-service` และ `sync-service-setting` ออกไป

permission: `registerService`

---

#### updateService

```graphql
mutation { updateService(serviceId: "<id>", update: { name: "New name", urlGetMetaData: "https://.../api/menuMetaData" }) { _id name } }
```

permission: `updateService`

---

#### resignService

ถอด service ออกจากระบบ · ต้องถอดออกจากทุกแอปก่อน (`removeServiceFromApp`) และถอด service ที่เป็น core set ไม่ได้

```graphql
mutation { resignService(serviceKey: "my-service") { serviceKey } }
```

permission: `resignService`

---

#### addServiceToApp

เพิ่ม service เข้าแอป (ระดับแอป) · สร้าง App Certificate (ส่ง `sync-app-certificate`) และ app service setting (คัดลอกจาก service setting ส่ง `sync-app-service-setting`) · ถ้า `refreshData: true` จะส่ง `refresh-data` ให้ทุก service ในแอป เพื่อให้ service ใหม่ได้ข้อมูลเดิมครบ

```graphql
mutation {
  addServiceToApp(input: { appKey: "my-app", serviceKeys: ["my-service"], refreshData: true }) {
    appKey serviceKey
  }
}
```

permission: `addServiceToApp`

---

#### removeServiceFromApp

ถอด service ออกจากแอป · ลบ App Certificate (ส่ง `sync-app-certificate` REMOVE) และ app service setting

```graphql
mutation { removeServiceFromApp(input: { appKey: "my-app", serviceKeys: ["my-service"] }) { appKey serviceKey } }
```

permission: `removeServiceFromApp`

---

#### refreshData

สั่งให้ service ในแอปส่งข้อมูลที่ตัวเองถือขึ้น Kafka ใหม่ (`type: ALL` ทุก service · `SELECT` เฉพาะที่ระบุใน `serviceKeys`)

```graphql
mutation { refreshData(input: { appKey: "my-app", type: SELECT, serviceKeys: ["access-control"] }) { serviceKey } }
```

permission: `refreshData`

---

#### generateSystemCertificate / resetSystemCertificate / revokedSystemCertificate

ออกใหม่ / รีเซ็ต / เพิกถอน System Certificate ของ service · `generate` และ `reset` คืน `publicKey`, `privateKey`, `systemCertificateId` ให้นำไปแทนไฟล์ใน `.gumon` ของ service

```graphql
mutation {
  resetSystemCertificate(resetSystemCertificateInput: { serviceKey: "my-service", systemCertificateId: "<id>" }) {
    systemCertificateId publicKey privateKey
  }
}
```

permission: `generateSystemCertificate` · `resetSystemCertificate` · `revokedSystemCertificate`

---

#### resetAppCertificate / resetAppCertificateByServiceKey / revokeAppCertificateByServiceKey

ออก App Certificate ใหม่ทั้งแอป / เฉพาะ service / เพิกถอนของ service ในแอป (1 แอป × 1 service มี App Certificate เดียว) · ผลถูกส่งออกทาง `sync-app-certificate`

```graphql
mutation { resetAppCertificateByServiceKey(appKey: "my-app", serviceKey: "my-service") { appKey serviceKey publicKey } }
```

permission: `resetAppCertificate` · `resetAppCertificateByServiceKey` · `revokeAppCertificateByServiceKey`

---

#### updateServiceSetting / resetServiceSetting

แก้ / คืนค่าเริ่มต้นของ service setting แล้วส่ง `sync-service-setting`

```graphql
mutation { updateServiceSetting(serviceKey: "notification", update: { setting: { exampleKey: "value" } }) { setting } }
```

permission: `updateServiceSetting` · `resetServiceSetting`

---

#### updateAppServiceSetting / resetAppServiceSetting

แก้ / คืนค่าเริ่มต้นของ app service setting แล้วส่ง `sync-app-service-setting`

```graphql
mutation { updateAppServiceSetting(appKey: "my-app", serviceKey: "authentication", update: { setting: { exampleKey: "value" } }) { setting } }
```

permission: `updateAppServiceSetting` · `resetAppServiceSetting`

---

#### handCheckService

สั่ง hand-check ใหม่ (`type: ALL` ทุก service · `SELECT` เฉพาะ `serviceKeys`) · core ส่ง `hand-check` ให้ service ปลายทาง

```graphql
mutation { handCheckService(input: { type: SELECT, serviceKeys: ["my-service"] }) { handCheckRefId serviceKey start } }
```

permission: `handCheckService`

---

## kafka consume Reference

ทุกข้อความมี header `appKey` (ถ้าเกี่ยวกับแอป) และ `serviceKey` = service **ปลายทาง** · payload รูป `{ action, <entity>: {...} }`

---

### register-service

service ประกาศตัวกับ core ตอนเริ่มทำงาน → core เริ่ม hand-check (ส่ง `hand-check`)

    topic: register-service
    header: serviceKey = core
    Action: ADD

| key ใน `registerService` | Type | คำอธิบาย |
| --- | --- | --- |
| systemCertificateId | string | id ของ System Certificate (จากไฟล์ `certificate-id.key`) |
| serviceKey | string | key ของ service ผู้ประกาศ |
| encryptData | string | systemCertificateId ที่เข้ารหัสด้วย `certificate.pub` |

---

### hand-check-result

คำตอบของ `hand-check` · core ถือว่าผ่านเมื่อ `resultData` ตรงกับ `handCheckRefId` แล้วอัปเดตสถานะ service ready

    topic: hand-check-result
    header: serviceKey = core

| key ใน `serviceHandCheckResult` | Type | คำอธิบาย |
| --- | --- | --- |
| handCheckRefId | string | id อ้างอิงที่ core สร้าง |
| systemCertificateId | string | |
| serviceKey | string | service ผู้ตอบ |
| encryptData | string | ข้อมูลเข้ารหัสที่ได้รับจาก core |
| resultData | string | ผลถอดรหัส (ถอดไม่ได้ให้ส่ง `""`) |

---

### refresh-data

เมื่อปลายทางเป็น `core`: ส่ง `sync-app-certificate`, `sync-service-setting`, `sync-app-service-setting`, `sync-service` และ `sync-permission` ของแอปนั้นออกไปใหม่ แล้วล้าง cache ของตัวเอง

    topic: refresh-data
    header: appKey, serviceKey = core

payload ดูที่ [refresh-data](#refresh-data_1) ในหัวข้อ produce

---

### sync-application

รับข้อมูลแอปจาก application service · ถ้าเป็นแอปใหม่ที่ active core จะผูก service core set ทุกตัวเข้าแอปให้อัตโนมัติ (สร้าง App Certificate + app service setting) แล้วส่ง `add-admin-app-role`

    topic: sync-application
    header: appKey
    Action: ADD | REMOVE

| key ใน `application` | Type | คำอธิบาย |
| --- | --- | --- |
| id | string | |
| appKey | string | |
| appName | string | |
| isActive | boolean | |
| isSystem | boolean | แอป SYSTEM |

---

### sync-app-credential

รับข้อมูลการเข้าใช้ของแอป (clientId, กุญแจตรวจ JWT, กฎของ token) จาก authentication service · ค่าลับถูกเข้ารหัสด้วย App Certificate ของปลายทาง · credential ชนิด `SYSTEM` (clientId + clientSecret) คือ apiKey สำหรับระบบภายนอก

    topic: sync-app-credential
    header: appKey, serviceKey = core
    Action: ADD | REMOVE

field ดูที่ [สิ่งที่ Service ต้องมี › sync-app-credential](serviceX.md)

---

### sync-user-policy

รับ UserPolicy พร้อมใช้จาก access-control เพื่อใช้ตรวจสิทธิ์ของ API ใน core

    topic: sync-user-policy
    header: appKey, serviceKey = core
    Action: ADD | REMOVE | REMOVE_APP | REMOVE_PERMISSION | REMOVE_USER | REMOVE_ORGANIZATION

field ดูที่ [สิ่งที่ Service ต้องมี › sync-user-policy](serviceX.md)

---

## Kafka Produce Reference

---

### init-system

ส่งครั้งเดียวตอนตั้งระบบจากศูนย์ (`GUMON_INIT_SYSTEM=true`) · **เฉพาะ core set** · ให้ core set ตัวอื่นสร้างข้อมูลตั้งต้นของแอป SYSTEM (แอป, App Certificate, app credential, ผู้ดูแลระบบ, app role)

    topic: init-system

---

### refresh-data

สั่งให้ service ปลายทางส่งข้อมูลที่ตัวเองถือขึ้นไปใหม่ (ให้ service ใหม่ในแอปทำงานต่อได้) และล้าง cache Redis ของตัวเอง · ส่งจาก `refreshData` หรือ `addServiceToApp(refreshData: true)`

    topic: refresh-data
    header: appKey, serviceKey = service ปลายทาง
    Action: ADD

| key ใน `refreshData` | Type | คำอธิบาย |
| --- | --- | --- |
| refreshDataId | string | id ของรอบ refresh (ใช้กันทำซ้ำ) |
| appKey | string | |
| serviceKey | string | service ปลายทาง |
| note | string | |

---

### hand-check

ถามว่า service ปลายทางยังถือกุญแจถูกต้องไหม · ส่งเมื่อได้รับ `register-service` หรือสั่ง `handCheckService` · ปลายทางถอด `encryptData` ด้วยไฟล์ `certificate` แล้วตอบ `hand-check-result`

    topic: hand-check
    header: serviceKey = service ปลายทาง
    Action: ADD

| key ใน `handCheck` | Type | คำอธิบาย |
| --- | --- | --- |
| handCheckRefId | string | id อ้างอิงที่ core สุ่ม |
| systemCertificateId | string | |
| serviceKey | string | |
| encryptData | string | handCheckRefId ที่เข้ารหัสด้วย System Certificate ของปลายทาง |

---

### sync-app-certificate

แจก App Certificate ของ service ในแอป · ส่งเมื่อ add / remove service ในแอป, reset / revoke app certificate, แอปใหม่ และ refresh-data

    topic: sync-app-certificate
    header: appKey, serviceKey = service ปลายทาง
    Action: ADD | REMOVE

| key ใน `appCertificate` | Type | คำอธิบาย |
| --- | --- | --- |
| id | string | |
| appKey | string | |
| serviceKey | string | service เจ้าของ certificate |
| publicKey | string | ใช้เข้ารหัสข้อมูลที่ส่งถึง service เจ้าของในแอปนี้ |
| privateKeyRsaEncrypt | string | privateKey ที่ถูกเข้ารหัส (ADD เท่านั้น) |
| symmetricKeyEncrypt | string | กุญแจถอด `privateKeyRsaEncrypt` เข้ารหัสด้วย System Certificate ของเจ้าของ (ADD เท่านั้น) |

---

### sync-service

ทะเบียน service · access-control ใช้ `urlGetMetaData` ดึงเมนูของหน้า admin ย่อย

    topic: sync-service
    header: serviceKey = service ปลายทาง
    Action: ADD

| key ใน `service` | Type |
| --- | --- |
| id, serviceKey, name, description, version, author | string |
| isCoreSet, isActive | boolean |
| type | `BACKEND` \| `MAIN_FRONTEND` \| `MICRO_FRONTEND` \| `OTHER` |
| urlFrontend, urlGetMetaData | string |

---

### sync-service-setting

JSON ตั้งค่าทั้งระบบของ service

    topic: sync-service-setting
    header: serviceKey = service ปลายทาง
    Action: ADD | REMOVE

| key ใน `serviceSetting` | Type |
| --- | --- |
| id | string |
| serviceKey | string |
| setting | JSON |

---

### sync-app-service-setting

JSON ตั้งค่าเฉพาะแอปของ service

    topic: sync-app-service-setting
    header: appKey, serviceKey = service ปลายทาง
    Action: ADD | REMOVE

| key ใน `appServiceSetting` | Type |
| --- | --- |
| id | string |
| appKey | string |
| serviceKey | string |
| setting | JSON |

---

### sync-permission

permission ของ API ใน core ส่งให้ access-control เพื่อผูกกับ role (ตอบ refresh-data)

    topic: sync-permission
    header: appKey, serviceKey = access-control
    Action: ADD

payload ดูที่ [สิ่งที่ Service ต้องมี › sync-permission](serviceX.md)

---

### add-admin-app-role

แอปใหม่ถูกสร้าง ⇒ ให้สร้าง app role ผู้ดูแล (`admin`) ของแอปนั้น

    topic: add-admin-app-role
    header: appKey, serviceKey = unit
    Action: ADD

| key ใน `adminAppRole` | Type |
| --- | --- |
| appKey | string |

---

> อัปเดตจากโค้ด gumon-core-service@d872cc3 · 2026-10-05
