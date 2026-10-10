# Application Service

Application Service เป็นเจ้าของข้อมูลแอป (Application) และ theme ของระบบ

- **Application** — แอปในระบบ (appKey, ชื่อ, รูป, สถานะ) · การสร้างแอปที่นี่จะส่ง `sync-application` ให้ core ผูก service core set ทุกตัวเข้าแอปใหม่โดยอัตโนมัติ
- **Theme** — theme กลางของทั้งระบบ (ต้นไม้ parent/child ผ่าน `themeKeyPath`)
- **AppTheme** — theme ที่แอปเลือกใช้
- **CredentialAppTheme** — theme ต่อ app credential (clientId)
- **HostTheme** — ผูก hostName → แอป + app credential + theme ให้หน้าบ้านรู้ว่าโดเมนนี้คือแอปไหน ใช้ clientId ไหน และใช้ theme อะไร

serviceKey: `application` · เป็นส่วนหนึ่งของ core set

- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)
- ดูเพิ่ม: [Core Service](coreService.md) · [สิ่งที่ Service ต้องมี](serviceX.md)

---

## API Reference

endpoint: `POST /graphql`

การยืนยันตัวตนที่ใช้ในหน้านี้

| ชนิด | header | ใช้กับ |
| --- | --- | --- |
| login | `Authorization: Bearer <accessToken>` + permission ตามที่ระบุ | mutation ทั้งหมด และ query ที่ระบุ permission |
| app credential | `X-APP-CLIENT-ID: <clientId>` (หรือ `Authorization`) | query อ่านข้อมูลแอป / theme ก่อน login |
| ไม่ต้องยืนยัน | – | ค้นหา HostTheme เพื่อ resolve โดเมน |

query ที่เป็นรายการรับ input รูป `{ filter, search, sort: { sortBy, sortOrder }, pagination: { page, limit } }` และคืน `{ <รายการ>[], pagination }`

---

### Query

---

#### getApplications

```graphql
query {
  getApplications(getAppInput: { filter: { isActive: true }, pagination: { page: 1, limit: 20 } }) {
    applications { _id appKey appName iconImageKey logoImageKey backgroundImageKey isActive isSystem createdAt }
    pagination { totalItems page limit }
  }
}
```

| field ของ Application | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | String | key ของแอป (ไม่ซ้ำ) |
| appName | String | ชื่อแอป |
| iconImageKey / logoImageKey / backgroundImageKey | String | key ไฟล์รูปใน storage |
| isActive | Boolean | |
| isSystem | Boolean | แอป SYSTEM (สร้างตอนตั้งระบบ) |

ยืนยันตัวตน: app credential

---

#### getApplicationById / getApplicationByAppKey

```graphql
query { getApplicationByAppKey(appKey: "my-app") { _id appKey appName isActive } }
```

ยืนยันตัวตน: app credential

---

#### getThemes / getThemeById / getThemeByThemeKey

```graphql
query {
  getThemes(getThemeInput: { filter: { isDefault: true } }) {
    themes { _id themeKey themeKeyPath themeName isDefault variable calVariable description }
  }
}
```

| field ของ Theme | Type | คำอธิบาย |
| --- | --- | --- |
| themeKey | String | key ของ theme |
| themeKeyPath | String | เส้นทาง parent ของ theme |
| themeName | String | |
| isDefault | Boolean | |
| variable | JSON | ค่าที่ theme นี้กำหนดเอง |
| calVariable | JSON | ค่าที่คำนวณรวมกับ theme แม่แล้ว (หน้าบ้านใช้ค่านี้) |

ยืนยันตัวตน: app credential

---

#### getAppThemes

```graphql
query { getAppThemes(getAppThemeInput: { filter: { appKey: "my-app" } }) { appThemes { _id appKey themeKey isDefault theme { themeName } } } }
```

permission: `getAppThemes`

---

#### getCredentialAppThemes / getCredentialAppThemeById

theme ต่อ app credential (`appKey`, `credentialId`, `themeKey`, `isDefault`, `theme`)

```graphql
query { getCredentialAppThemes(getCredentialAppThemeInput: { filter: { appKey: "my-app" } }) { credentialAppThemes { credentialId themeKey isDefault } } }
```

ยืนยันตัวตน: app credential

---

#### getConfigByHost

ค่าตั้งต้นของหน้าบ้านตาม host: credentialId, theme และ app service setting ของแอป

```graphql
query { getConfigByHost(getConfigByHostInput: { host: "admin.example.com" }) { credentialId theme { themeKey calVariable } appServiceSetting { serviceKey setting } } }
```

ยืนยันตัวตน: app credential

---

#### getHostThemes

```graphql
query {
  getHostThemes(getHostThemeInput: { filter: { appKey: "my-app" } }) {
    hostThemes { _id hostThemeKey hostName appKey clientId themeKey isDefault isActive }
  }
}
```

permission: `getHostThemes`

---

#### getHostThemeByHostName / getHostThemeById / getHostThemeByKey

หน้าบ้านเรียกตอนเปิดเว็บเพื่อรู้ว่าโดเมนนี้คือแอปไหน ใช้ clientId ไหน และ theme อะไร (เรียกได้ก่อน login)

```graphql
query {
  getHostThemeByHostName(hostName: "admin.example.com") {
    appKey clientId hostThemeKey
    application { appKey appName }
    appCredential { clientId appCredentialType }
    theme { themeKey calVariable }
  }
}
```

| field ของ HostTheme | Type | คำอธิบาย |
| --- | --- | --- |
| hostThemeKey | String | key ของ host theme |
| hostName | String | โดเมน |
| appKey / applicationId / application | | แอปที่ผูก |
| appCredentialId / clientId / appCredential | | app credential ที่หน้าบ้านต้องใช้ |
| themeId / themeKey / themeKeyPath / theme | | theme ที่ใช้ |
| isDefault / isActive | Boolean | |

---

### Mutation

---

#### createApplication

สร้างแอปใหม่ แล้วส่ง `sync-application` (ADD) ⇒ core ผูก service core set ทุกตัวเข้าแอป และสร้าง app role ผู้ดูแลให้อัตโนมัติ

```graphql
mutation {
  createApplication(createApplicationInput: { appKey: "my-app", appName: "My App", isActive: true }) {
    _id appKey appName
  }
}
```

| input | Type | คำอธิบาย |
| --- | --- | --- |
| appKey | String! | ห้ามซ้ำ |
| appName | String! | |
| iconImageKey / logoImageKey / backgroundImageKey | String | |
| isActive | Boolean = true | |

permission: `createApplication`

---

#### updateApplication

แก้ข้อมูลแอป (`appName`, รูป, `isActive`) แล้วส่ง `sync-application` (ADD)

```graphql
mutation { updateApplication(id: "<id>", update: { appName: "New name" }) { appKey appName } }
```

permission: `updateApplication`

---

#### deleteApplication

ลบแอป แล้วส่ง `sync-application` (REMOVE)

```graphql
mutation { deleteApplication(id: "<id>") { appKey } }
```

permission: `deleteApplication`

---

#### createTheme / updateTheme / deleteTheme / resetTheme

จัดการ theme กลาง · `parentThemeId` ทำให้ theme สืบค่าจาก theme แม่ · `resetTheme` คำนวณ `calVariable` ของทุก theme ใหม่

```graphql
mutation {
  createTheme(createThemeInput: { themeName: "Dark Blue", parentThemeId: null, variable: { colorPrimary: "#1d4ed8" }, isDefault: false }) {
    _id themeKey themeKeyPath calVariable
  }
}
```

permission: `createTheme` · `updateTheme` · `deleteTheme` · `resetTheme`

---

#### createAppTheme / deleteAppTheme

เลือก theme ให้แอป

```graphql
mutation { createAppTheme(createAppThemeInput: { appKey: "my-app", themeKey: "<themeKey>", isDefault: true }) { _id appKey themeKey } }
```

permission: `createAppTheme` · `deleteAppTheme`

---

#### createCredentialAppTheme / deleteCredentialAppTheme

เลือก theme ให้ app credential

```graphql
mutation { createCredentialAppTheme(createCredentialAppThemeInput: { appKey: "my-app", credentialId: "<appCredentialId>", themeKey: "<themeKey>" }) { _id } }
```

ใช้โดยผู้ดูแลแอป

---

#### createHostTheme / updatedHostTheme / deleteHostTheme

ผูกโดเมนกับแอป + app credential + theme

```graphql
mutation {
  createHostTheme(createHostThemeInput: { hostName: "admin.example.com", appCredentialId: "<appCredentialId>", themeKey: "<themeKey>", isDefault: true }) {
    _id hostThemeKey hostName appKey clientId
  }
}
```

| input (create) | Type | คำอธิบาย |
| --- | --- | --- |
| hostName | String! | โดเมน |
| appCredentialId | String! | app credential ที่หน้าบ้านจะใช้ |
| themeKey | String! | |
| hostThemeKey | String | ไม่ระบุได้ |
| isDefault | Boolean = false | |
| isActive | Boolean = true | |
| description | String | |

`updatedHostTheme(hostThemeId, updateHostThemeInput: { isDefault, themeKey, description, isActive })`

permission: `createHostTheme` · `updateHostTheme` · `deleteHostTheme`

---

## kafka consume Reference

header ทุกข้อความ: `appKey` และ `serviceKey` = service ปลายทาง · topic ที่ส่งถึง service เดียวจะทำงานเฉพาะเมื่อ `serviceKey` = `application` และ application มี App Certificate ในแอปนั้น

| topic | ผู้ส่ง | ทำอะไร |
| --- | --- | --- |
| `init-system` | core | ตั้งระบบครั้งแรก: สร้างแอป SYSTEM, เก็บ App Certificate / app credential และสร้าง Theme / AppTheme / CredentialAppTheme ตั้งต้น |
| `refresh-data` | core | ส่ง `sync-application` ของแอปนั้น + `sync-permission` ขึ้นไปใหม่ แล้วล้าง cache ของตัวเอง |
| `sync-app-certificate` | core | เก็บ App Certificate ของ service ในแอป |
| `sync-app-credential` | authentication | เก็บ app credential (ใช้ตรวจ token / clientId) |
| `sync-service-setting` | core | เก็บ service setting ของ application |
| `sync-app-service-setting` | core | เก็บ app service setting (ใช้ตอบ `getConfigByHost`) |
| `sync-user-policy` | access-control | เก็บ UserPolicy ใช้ตรวจ permission ของ API |

รายละเอียด payload ของแต่ละ topic ดูที่ [สิ่งที่ Service ต้องมี](serviceX.md)

---

## Kafka Produce Reference

---

### sync-application

ข้อมูลแอป กระจายให้ทุก service (รวม core)

    topic: sync-application
    header: appKey
    Action: ADD (สร้าง / แก้ไข) | REMOVE (ลบ)

| key ใน `application` | Type | คำอธิบาย |
| --- | --- | --- |
| id | string | |
| appKey | string | |
| appName | string | |
| description | string | |
| iconImageKey / logoImageKey / backgroundImageKey | string | |
| isActive | boolean | |
| isSystem | boolean | |

---

### sync-permission

permission ของ API ใน application ส่งให้ access-control (ตอบ refresh-data)

    topic: sync-permission
    header: appKey, serviceKey = access-control
    Action: ADD

payload ดูที่ [สิ่งที่ Service ต้องมี › sync-permission](serviceX.md)

---

> อัปเดตจากโค้ด gumon-application-service@45a5f62 · 2026-10-05
