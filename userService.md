# Profile Service

Service สำหรับเก็บข้อมูลโปรไฟล์ของผู้ใช้ในแต่ละแอป (เดิมหน้านี้ชื่อ User Service)

    serviceKey: profile

- **Profile** — ชื่อ (`firstName`, `middleName`, `lastName`, `displayName`), เพศ (`gender`: `MALE`, `FEMALE`, `NOT_SPECIFIED`), รูปโปรไฟล์ (`profileImage`), ลายเซ็นอิเล็กทรอนิกส์ (`electronicSignatureKey`) และองค์กรเริ่มต้น (`defaultOrganizationId`, `defaultOrganizationKey`)
- **Metadata / Profile Custom** — แอปกำหนดฟิลด์เสริมของโปรไฟล์เองได้ (`createMetadata`) โดยไม่ต้องแก้โค้ด แล้วเก็บค่าของผู้ใช้แต่ละคนใน profile custom · ชนิดฟิลด์: `STRING`, `NUMBER`, `SINGLE_CHOICE`, `MULTIPLE_CHOICE`, `DATE`, `DATE_TIME`, `TIME`, `BOOLEAN`

บัญชีผู้ใช้ (username, email, เบอร์โทร, รหัสผ่าน, การ login) อยู่ที่ [Authentication Service](authenticationService.md) · เมื่อมีการสมัครบัญชี Authentication Service จะส่งข้อมูลเริ่มต้นมาทาง topic `sync-auth` แล้ว Profile Service สร้างโปรไฟล์ให้

<br>

- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)

---

<br>
<br>

## API Reference

การยืนยันตัวตนและ header ดูที่ [Authentication Service](authenticationService.md#app-credential) · "สิทธิ์" คือ permissionKey ของ service `profile` ที่ได้รับผ่าน role ใน [ACL Service](aclService.md)

---

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

---

#### getMetadataFields

ดึงนิยามฟิลด์เสริม (metadata) ของโปรไฟล์ในแอปปัจจุบัน แบบแบ่งหน้า

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getMetadataProfile`

```graphql
query GetMetadataFields($input: GetMetadataFieldInput) {
  getMetadataFields(input: $input) {
    metadataFields { _id fieldLabel fieldKey fieldType min max }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetMetadataFieldInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetMetadataFieldFilterInput` |  |  |
| search | `GetMetadataFieldSearchInput` |  |  |
| sort | `GetMetadataFieldSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `MetadataFieldPagination!`

---

#### getMetadataById

ดึงนิยามฟิลด์เสริมตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getMetadataProfileById`

```graphql
query GetMetadataById($metadataId: ID!) {
  getMetadataById(metadataId: $metadataId) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| metadataId | `ID!` |

Response: `Metadata!`

---

#### getMetadataByKey

ดึงนิยามฟิลด์เสริมตาม key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getMetadataProfileByKey`

```graphql
query GetMetadataByKey($metadataKey: String!) {
  getMetadataByKey(metadataKey: $metadataKey) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| metadataKey | `String!` |

Response: `Metadata!`

---

#### getMetadataFieldsByAppKey

ดึงนิยามฟิลด์เสริมของแอปที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMetadataFieldsByAppKey($appKey: String!, $input: GetMetadataFieldInput) {
  getMetadataFieldsByAppKey(appKey: $appKey, input: $input) {
    metadataFields { _id fieldLabel fieldKey fieldType min max }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetMetadataFieldInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetMetadataFieldFilterInput` |  |  |
| search | `GetMetadataFieldSearchInput` |  |  |
| sort | `GetMetadataFieldSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String!`

Response: `MetadataFieldPagination`

---

#### getMetadataByIdSystem

ดึงนิยามฟิลด์เสริมตาม id โดยระบุ appKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMetadataByIdSystem($appKey: String!, $metadataId: ID!) {
  getMetadataByIdSystem(appKey: $appKey, metadataId: $metadataId) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appKey | `String!` |
| metadataId | `ID!` |

Response: `Metadata!`

---

#### getMetadataByKeySystem

ดึงนิยามฟิลด์เสริมตาม key โดยระบุ appKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMetadataByKeySystem($appKey: String!, $metadataKey: String!) {
  getMetadataByKeySystem(appKey: $appKey, metadataKey: $metadataKey) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appKey | `String!` |
| metadataKey | `String!` |

Response: `Metadata!`

---

#### getProfileCustom

ดึงค่าฟิลด์เสริมของผู้ใช้ตาม authId

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfileCustom`

```graphql
query GetProfileCustom($authId: String!) {
  getProfileCustom(authId: $authId) {
    value
    metadata { _id fieldLabel fieldKey fieldType min max }
    updatedAt
    updatedBy
  }
}
```

| argument | Type |
| --- | --- |
| authId | `String!` |

Response: `[ProfileCustom]!`

---

#### getMyProfileCustom

ดึงค่าฟิลด์เสริมของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyProfileCustom {
  getMyProfileCustom {
    value
    metadata { _id fieldLabel fieldKey fieldType min max }
    updatedAt
    updatedBy
  }
}
```

Response: `[ProfileCustom]!`

---

#### getMyProfile

ดึงโปรไฟล์ของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyProfile {
  getMyProfile {
    _id
    appKey
    firstName
    middleName
    lastName
    displayName
    gender
    authId
    username
    profileImage
    # ...
  }
}
```

Response: `Profile!`

---

#### getProfileById

ดึงโปรไฟล์ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfileById`

```graphql
query GetProfileById($profileId: ID!) {
  getProfileById(profileId: $profileId) {
    _id
    appKey
    firstName
    middleName
    lastName
    displayName
    gender
    authId
    username
    profileImage
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| profileId | `ID!` |

Response: `Profile!`

---

#### getProfiles

ดึงโปรไฟล์ทั้งหมดในแอปปัจจุบัน แบบแบ่งหน้า

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfiles`

```graphql
query GetProfiles($input: GetProfilesInput) {
  getProfiles(input: $input) {
    profiles { _id appKey firstName middleName lastName displayName }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetProfilesInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetProfilesFilterInput` |  |  |
| search | `GetProfilesSearchInput` |  |  |
| sort | `GetProfilesSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `ProfilesPagination!`

---

#### getProfilesByAppKey

ดึงโปรไฟล์ของแอปที่ระบุ แบบแบ่งหน้า

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfilesByAppKey`

```graphql
query GetProfilesByAppKey($appKey: String!, $input: GetProfilesInput) {
  getProfilesByAppKey(appKey: $appKey, input: $input) {
    profiles { _id appKey firstName middleName lastName displayName }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetProfilesInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetProfilesFilterInput` |  |  |
| search | `GetProfilesSearchInput` |  |  |
| sort | `GetProfilesSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String!`

Response: `ProfilesPagination!`

---

#### getProfileByIdSystem

ดึงโปรไฟล์ตาม id โดยระบุ appKey (ใช้ข้ามแอป)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfileById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetProfileByIdSystem($profileId: ID!, $appKey: String) {
  getProfileByIdSystem(profileId: $profileId, appKey: $appKey) {
    _id
    appKey
    firstName
    middleName
    lastName
    displayName
    gender
    authId
    username
    profileImage
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| profileId | `ID!` |
| appKey | `String` |

Response: `Profile!`

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

---

#### createMetadata

สร้างนิยามฟิลด์เสริมของโปรไฟล์

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createMetadataProfile`

```graphql
mutation CreateMetadata($createMetadataInput: CreateMetadataInput!) {
  createMetadata(createMetadataInput: $createMetadataInput) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

`createMetadataInput`: `CreateMetadataInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  | appKey ที่ต้องการสร้าง metadata ไม่ใส่ใช้ appKey จาก login |
| fieldLabel | `String!` | ใช่ | The Label when display in the form. |
| fieldKey | `String!` | ใช่ | The Key of the metadata. |
| fieldType | `EnumFieldTypes!` | ใช่ | The Type of the metadata. |
| fieldOptions | `[FieldOptionsInput]` |  | The Options when the field type is SINGLE_CHOICE or MULTIPLE_CHOICE. Example: - SINGLE_CHOICE: - "fieldOptions": [{"key": "test1","value": "test1"}] - MULTIPLE_CHOICE: - "fieldOptions": [{"key": "test1","value": "test1"}] |
| min | `Float` |  | The min value of the metadata. |
| max | `Float` |  | The max value of the metadata. |
| maxFileSize | `Int` |  | The max file size of the metadata. |
| default | `JSON!` | ใช่ | The default value in form. Example: - STRING: "text" - NUMBER: 0 - SINGLE_CHOICE: - "default": {"key": "test1","value": "test1"} - MULTIPLE_CHOICE: - "default": [{"key": "test1","value": "test1"}] - DATE: "2021-01-01" - DATE_TIME: "2021-01-01T00:00:00.000Z" - TIME: "2021-01-01T00:00:00.000Z" |
| systemDescription | `String` |  | The text for explain about base system metadata. |
| displayDescription | `String` |  | The text for explain about metadata. |
| placeHolderText | `String` |  | The placeHolder for description of metadata. |
| isActive | `Boolean!` | ใช่ | The status of metadata. |
| isRequired | `Boolean!` | ใช่ | The field is required or not. |
| isSensitive | `Boolean!` | ใช่ | The field is sensitive or not. |
| ordinalNumber | `Int!` | ใช่ | The order when sorting metadata. |

Response: `Metadata!`

---

#### updateMetadata

แก้ไขนิยามฟิลด์เสริม (ส่งได้หลายรายการ)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateMetadataProfile`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateMetadata($updateMetadataInput: [UpdateMetadataInput]!, $appKey: String) {
  updateMetadata(updateMetadataInput: $updateMetadataInput, appKey: $appKey) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

`updateMetadataInput`: `UpdateMetadataInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| metadataId | `String!` | ใช่ | The ID of the metadata. |
| fieldLabel | `String` |  | The Label when display in the form. |
| default | `JSON` |  | The default value in form. Example: - STRING: "text" - NUMBER: 0 - SINGLE_CHOICE: - "default": {"key": "test1","value": "test1"} - MULTIPLE_CHOICE: - "default": [{"key": "test1","value": "test1"}] - DATE: "2021-01-01" - DATE_TIME: "2021-01-01T00:00:00.000Z" - TIME: "2021-01-01T00:00:00.000Z" |
| systemDescription | `String` |  | The text for explain about metadata. |
| displayDescription | `String` |  | The text for explain about metadata. |
| placeHolderText | `String` |  | The placeHolder for description of metadata. |
| isActive | `Boolean` |  | The status of metadata. |
| isRequired | `Boolean` |  | The field is required or not. |
| ordinalNumber | `Int` |  | the order when sorting metadata. |

argument อื่น: `appKey: String`

Response: `[Metadata]!`

---

#### deleteMetadata

ลบนิยามฟิลด์เสริมตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteMetadataProfile`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation DeleteMetadata($metadataId: ID!, $appKey: String) {
  deleteMetadata(metadataId: $metadataId, appKey: $appKey) {
    _id
    fieldLabel
    fieldKey
    fieldType
    fieldOptions { key value }
    min
    max
    maxFileSize
    default
    systemDescription
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| metadataId | `ID!` |
| appKey | `String` |

Response: `Metadata!`

---

#### updateProfileCustom

แก้ไขค่าฟิลด์เสริมของผู้ใช้ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateProfileCustoms`

```graphql
mutation UpdateProfileCustom($updateProfileCustomInput: [UpdateProfileCustomInput]!) {
  updateProfileCustom(updateProfileCustomInput: $updateProfileCustomInput) {
    value
    metadata { _id fieldLabel fieldKey fieldType min max }
    updatedAt
    updatedBy
  }
}
```

`updateProfileCustomInput`: `UpdateProfileCustomInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String!` | ใช่ | specify application key of the profile custom field |
| authId | `String!` | ใช่ | specify auth ID of the profile custom field |
| fieldMetadataId | `String!` | ใช่ | specify field metadata ID of the profile custom field |
| value | `JSON` |  | specify value of the profile custom field |

Response: `[ProfileCustom]!`

---

#### updateMyProfileCustom

แก้ไขค่าฟิลด์เสริมของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UpdateMyProfileCustom($UpdateMyProfileCustomInput: [UpdateMyProfileCustomInput]!) {
  updateMyProfileCustom(UpdateMyProfileCustomInput: $UpdateMyProfileCustomInput) {
    value
    metadata { _id fieldLabel fieldKey fieldType min max }
    updatedAt
    updatedBy
  }
}
```

`UpdateMyProfileCustomInput`: `UpdateMyProfileCustomInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| fieldMetadataId | `String!` | ใช่ | specify field metadata ID of the profile custom field |
| value | `JSON` |  | specify value of the profile custom field |

Response: `[ProfileCustom]!`

---

#### updateProfile

แก้ไขโปรไฟล์ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateProfile`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateProfile($profileId: ID!, $updateProfileInput: UpdateProfileInput!, $appKey: String) {
  updateProfile(profileId: $profileId, updateProfileInput: $updateProfileInput, appKey: $appKey) {
    _id
    appKey
    firstName
    middleName
    lastName
    displayName
    gender
    authId
    username
    profileImage
    # ...
  }
}
```

`updateProfileInput`: `UpdateProfileInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| firstName | `String` |  | first name of the profile. |
| middleName | `String` |  | middle name of the profile. |
| lastName | `String` |  | last name of the profile. |
| displayName | `String` |  | display name of the profile. |
| gender | `EnumGender` |  | gender of the profile. |
| profileImage | `String` |  | profile image of the profile. |
| electronicSignatureKey | `String` |  | file key of electronic signature. |

argument อื่น: `profileId: ID!`, `appKey: String`

Response: `Profile!`

---

#### updateMyProfile

แก้ไขโปรไฟล์ของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UpdateMyProfile($updateProfileInput: UpdateProfileInput!) {
  updateMyProfile(updateProfileInput: $updateProfileInput) {
    _id
    appKey
    firstName
    middleName
    lastName
    displayName
    gender
    authId
    username
    profileImage
    # ...
  }
}
```

`updateProfileInput`: `UpdateProfileInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| firstName | `String` |  | first name of the profile. |
| middleName | `String` |  | middle name of the profile. |
| lastName | `String` |  | last name of the profile. |
| displayName | `String` |  | display name of the profile. |
| gender | `EnumGender` |  | gender of the profile. |
| profileImage | `String` |  | profile image of the profile. |
| electronicSignatureKey | `String` |  | file key of electronic signature. |

Response: `Profile!`

---

<br>
<br>

## Kafka consume Reference

ทุกข้อความมี header `appKey` และ `serviceKey` (service ปลายทาง) · payload อยู่ในรูป `{ <ข้อมูล>: {...}, action: "ADD" | "REMOVE" | ... }` · ข้อความของแอปที่ service นี้ไม่มี AppCertificate จะถูกข้าม

---

### topic มาตรฐาน

| topic | ใช้ทำอะไร |
| --- | --- |
| `init-system` | ตั้งระบบจากศูนย์ (เฉพาะ core set) |
| `refresh-data` | core สั่งให้ส่งข้อมูลที่ถืออยู่ขึ้นไปใหม่ (`sync-profile` และ `sync-permission`) + ล้าง cache |
| `sync-app-certificate` | รับ AppCertificate ของ service ต่อแอป จาก core |
| `sync-app-credential` | รับ AppCredential ของแอป จาก Authentication Service ใช้ตรวจ token / header |
| `sync-service-setting` / `sync-app-service-setting` | รับค่าตั้งค่าเพิ่มเติมแบบ JSON ทั้งระบบ / รายแอป |
| `sync-application` | รับข้อมูลแอป |
| `sync-user-policy` | รับ UserPolicy ของ permission `profile` จาก ACL ใช้ตรวจสิทธิ์ |
| `sync-organization` | รับข้อมูลองค์กรจาก [Unit Service](unitService.md) |

---

### sync-auth

รับข้อมูลบัญชีจาก [Authentication Service](authenticationService.md) แล้วสร้าง / อัปเดต / ลบโปรไฟล์

    topic: sync-auth

| key | Type | คำอธิบาย |
| --- | --- | --- |
| account.appKey | string | appKey |
| account.authId | string | id ของบัญชี |
| account.username | string | username |
| account.firstName / middleName / lastName / displayName | string | ชื่อ |
| account.gender | string | เพศ |
| account.profileImage | string | fileKey ของรูปโปรไฟล์ |
| account.electronicSignatureKey | string | fileKey ของลายเซ็น |
| account.roleKey | string | `ADMIN`, `NONE` |
| account.defaultOrganizationId / defaultOrganizationKey | string | องค์กรเริ่มต้น |
| action | string | `ADD`, `REMOVE` |

ค่า message ถูกห่อเป็น `{ "value": "<JSON string>" }` ต้อง parse ค่า `value` อีกชั้น (ดู [Authentication Service](authenticationService.md#sync-auth))

---

### set-profile

ให้ service อื่นสร้าง / แก้โปรไฟล์ของผู้ใช้ผ่าน Kafka

    topic: set-profile

| key | Type | คำอธิบาย |
| --- | --- | --- |
| account.appKey | string | appKey |
| account.authId | string | ผู้ใช้ |
| account.username | string | username |
| account.emails | string[] | email |
| account.firstName / middleName / lastName / displayName | string | ชื่อ |
| account.gender | string | `MALE`, `FEMALE`, `NOT_SPECIFIED` |
| account.profileImage | string | fileKey ของรูปโปรไฟล์ |
| account.roleKey | string | role |
| action | string | `ADD`, `REMOVE` |

---

<br>
<br>

## Kafka Produce Reference

---

### sync-profile

ส่งข้อมูลโปรไฟล์หลังมีการสร้าง / แก้ / ลบ ให้ service อื่นที่ต้องใช้ชื่อหรือรูปของผู้ใช้ (เช่น ACL Service, Notification Service)

    topic: sync-profile

| key | Type | คำอธิบาย |
| --- | --- | --- |
| profile.id | string | id ของโปรไฟล์ |
| profile.appKey | string | appKey |
| profile.authId | string | id ของบัญชี |
| profile.username | string | username |
| profile.firstName / middleName / lastName / displayName | string | ชื่อ |
| profile.gender | string | เพศ |
| profile.profileImage | string | fileKey ของรูปโปรไฟล์ |
| profile.electronicSignatureKey | string | fileKey ของลายเซ็น |
| profile.defaultOrganizationId / defaultOrganizationKey | string | องค์กรเริ่มต้น |
| action | string | `ADD`, `REMOVE` |

---

### sync-permission

ส่ง permission ของ `profile` ให้ [ACL Service](aclService.md) (ตอนได้ `refresh-data`)

    topic: sync-permission

---

> อัปเดตจากโค้ด gumon-profile-service@c4b90a4 · 2026-10-05
