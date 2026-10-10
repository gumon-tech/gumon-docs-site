# Unit Service

Service สำหรับจัดการโครงสร้างองค์กรและ role ของแต่ละแอป

    serviceKey: unit

หน้าที่หลัก

- **Organization** — องค์กร / หน่วยงาน เป็นโครงต้นไม้ (`parentOrganizationId`, `organizationPath`) มีองค์กรสาธารณะ (`isPublic`) และการอนุมัติองค์กร
- **Organization Type / Organization Tag** — ประเภทและแท็กขององค์กร (เป็นต้นไม้เช่นกัน)
- **Organization Approve** — ใบอนุมัติองค์กรและรายละเอียดประกอบ สถานะ `INPROGRESS`, `APPROVED`, `REJECTED`, `NEED_MORE_INFORMATION`
- **Contact** — ผู้ติดต่อขององค์กร (`organizationContact`), บุคคลติดต่อ (`organizationContactPeople`), ความสัมพันธ์ (`organizationContactRelation`) และประเภทลูกค้า (`customerType`)
- **Role** — นิยาม `appRole` (ทั้งแอป), `organizationRole` (ต่อองค์กร) และ `defaultOrganizationRole` (แม่แบบ role ที่ทุกองค์กรใหม่จะได้อัตโนมัติ) · การผูก permission / เมนู / ผู้ใช้เข้ากับ role ทำที่ [ACL Service](aclService.md)
- **Custom Running Number** — เลขรันนิ่ง (เช่น เลขเอกสาร) ระดับแอปหรือองค์กร ตาม pattern / รอบ ปี-เดือน-วัน

หน้าที่ของ Label Service เดิม (label ขององค์กร หน่วยงาน ตำแหน่ง role) ถูกรวมมาไว้ที่ service นี้แล้ว ในรูปของ organization, organization type / tag และ role ข้างบน · ดู [Label Service (เลิกใช้)](labelService.md)

การสร้างองค์กรใหม่ จะสร้าง organizationRole จาก defaultOrganizationRole ทุกตัวให้อัตโนมัติ และสร้างใบอนุมัติองค์กร

<br>

- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)

---

<br>
<br>

## API Reference

การยืนยันตัวตนและ header ดูที่ [Authentication Service](authenticationService.md#app-credential) · "สิทธิ์" คือ permissionKey ของ service `unit` ที่ได้รับผ่าน role ใน [ACL Service](aclService.md) · "ระดับองค์กร" หมายถึงสิทธิ์ที่ให้แยกตามองค์กร

นอกจาก GraphQL มี REST สำหรับนำเข้าผู้ติดต่อจากไฟล์ CSV: `POST /organization-contact/import` (multipart: `file` เป็น CSV, `organizationKey`) · ต้อง login · สิทธิ์ `importOrganizationContact`

---

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

---

#### getAppRoles

ดึงข้อมูล appRoles ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRoles`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetAppRoles($input: GetAppRoleInput) {
  getAppRoles(input: $input) {
    appRoles { _id appRoleKey title subTitle description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAppRoleFilterInput` |  |  |
| search | `GetAppRoleSearchInput` |  |  |
| sort | `GetAppRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `AppRolePagination!`

---

#### getAppRoleById

ดึงข้อมูล appRoles ตาม ID ถ้าอยากค้นหาข้าม appKey จำเป็นต้องระบุ input.appKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRoleById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetAppRoleById($appRoleId: ID!, $appKey: String) {
  getAppRoleById(appRoleId: $appRoleId, appKey: $appKey) {
    _id
    appRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRoleId | `ID!` |
| appKey | `String` |

Response: `AppRole!`

---

#### getAppRoleByKey

ดึงข้อมูล appRoles ตาม appRoleKey ถ้าอยากค้นหาข้าม appKey จำเป็นต้องระบุ input.appKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRoleByKey`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetAppRoleByKey($appRoleKey: String!, $appKey: String) {
  getAppRoleByKey(appRoleKey: $appRoleKey, appKey: $appKey) {
    _id
    appRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRoleKey | `String!` |
| appKey | `String` |

Response: `AppRole!`

---

#### getCustomRunningNumberLogs

ดึงข้อมูล custom running number log ทั้งหมด

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberLogs`

```graphql
query GetCustomRunningNumberLogs($getInput: GetCustomRunningNumberLogInput) {
  getCustomRunningNumberLogs(getInput: $getInput) {
    customRunningNumberLogs { _id organizationId organizationKey customRunningNumberId customRunningNumberKey generatedCode }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomRunningNumberLogInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomRunningNumberLogFilterInput` |  |  |
| search | `GetCustomRunningNumberLogSearchInput` |  |  |
| sort | `GetCustomRunningNumberLogSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `CustomRunningNumberLogPagination`

---

#### getCustomRunningNumberLogByOrganizationKey

ดึงข้อมูل custom running number log ตาม organization key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberLogByOrganizationKey` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberLogByOrganizationKey($organizationKey: String!, $getInput: GetCustomRunningNumberLogByOrganizationKeyInput) {
  getCustomRunningNumberLogByOrganizationKey(organizationKey: $organizationKey, getInput: $getInput) {
    customRunningNumberLogs { _id organizationId organizationKey customRunningNumberId customRunningNumberKey generatedCode }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomRunningNumberLogByOrganizationKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomRunningNumberLogByOrganizationKeyFilterInput` |  |  |
| search | `GetCustomRunningNumberLogByOrganizationKeySearchInput` |  |  |
| sort | `GetCustomRunningNumberLogSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `organizationKey: String!`

Response: `CustomRunningNumberLogPagination`

---

#### getCustomRunningNumberLogById

ดึงข้อมูล custom running number log ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberLogById` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberLogById($customRunningNumberLogId: String!) {
  getCustomRunningNumberLogById(customRunningNumberLogId: $customRunningNumberLogId) {
    _id
    organizationId
    organizationKey
    customRunningNumberId
    customRunningNumberKey
    generatedCode
    createdBy
    updatedBy
    createdAt
    updatedAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customRunningNumberLogId | `String!` |

Response: `CustomRunningNumberLog`

---

#### getCustomRunningNumberLogByCustomRunningNumberId

ดึงข้อมูล custom running number log ตาม custom running number id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberLogByCustomRunningNumberId` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberLogByCustomRunningNumberId($customRunningNumberId: String!, $getInput: GetCustomRunningNumberLogByCustomRunningNumberIdInput) {
  getCustomRunningNumberLogByCustomRunningNumberId(customRunningNumberId: $customRunningNumberId, getInput: $getInput) {
    customRunningNumberLogs { _id organizationId organizationKey customRunningNumberId customRunningNumberKey generatedCode }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomRunningNumberLogByCustomRunningNumberIdInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomRunningNumberLogByCustomRunningNumberIdFilterInput` |  |  |
| search | `GetCustomRunningNumberLogByCustomRunningNumberIdSearchInput` |  |  |
| sort | `GetCustomRunningNumberLogSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `customRunningNumberId: String!`

Response: `CustomRunningNumberLogPagination`

---

#### getCustomRunningNumbers

ดึงข้อมูล custom running number ทั้งหมด

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumbers`

```graphql
query GetCustomRunningNumbers($getInput: GetCustomRunningNumberInput) {
  getCustomRunningNumbers(getInput: $getInput) {
    customRunningNumbers { _id level organizationId organizationKey customRunningNumberKey refKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomRunningNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomRunningNumberFilterInput` |  |  |
| search | `GetCustomRunningNumberSearchInput` |  |  |
| sort | `GetCustomRunningNumberSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `CustomRunningNumberPagination`

---

#### getCustomRunningNumberByOrganizationKey

ดึงข้อมูล custom running number ตาม organization key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberByOrganizationKey` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberByOrganizationKey($organizationKey: String!, $getInput: GetCustomRunningNumberByOrganizationKeyInput) {
  getCustomRunningNumberByOrganizationKey(organizationKey: $organizationKey, getInput: $getInput) {
    customRunningNumbers { _id level organizationId organizationKey customRunningNumberKey refKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomRunningNumberByOrganizationKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomRunningNumberByOrganizationKeyFilterInput` |  |  |
| search | `GetCustomRunningNumberByOrganizationKeySearchInput` |  |  |
| sort | `GetCustomRunningNumberSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `organizationKey: String!`

Response: `CustomRunningNumberPagination`

---

#### getCustomRunningNumberById

ดึงข้อมูล custom running number ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberById` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberById($customRunningNumberId: String!) {
  getCustomRunningNumberById(customRunningNumberId: $customRunningNumberId) {
    _id
    level
    organizationId
    organizationKey
    customRunningNumberKey
    refKey
    order
    title
    description
    serial
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customRunningNumberId | `String!` |

Response: `CustomRunningNumber`

---

#### getCustomRunningNumberByKey

ดึงข้อมูล custom running number ตาม key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberByKey` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberByKey($customRunningNumberKey: String!) {
  getCustomRunningNumberByKey(customRunningNumberKey: $customRunningNumberKey) {
    _id
    level
    organizationId
    organizationKey
    customRunningNumberKey
    refKey
    order
    title
    description
    serial
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customRunningNumberKey | `String!` |

Response: `CustomRunningNumber`

---

#### getCustomRunningNumberByRefKey

ดึงข้อมูล custom running number ตาม ref key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomRunningNumberByRefKey` (ระดับองค์กร)

```graphql
query GetCustomRunningNumberByRefKey($refKey: String!, $getInput: GetCustomRunningNumberByRefKeyInput) {
  getCustomRunningNumberByRefKey(refKey: $refKey, getInput: $getInput) {
    customRunningNumbers { _id level organizationId organizationKey customRunningNumberKey refKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomRunningNumberByRefKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomRunningNumberByRefKeyFilterInput` |  |  |
| search | `GetCustomRunningNumberByRefKeySearchInput` |  |  |
| sort | `GetCustomRunningNumberSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `refKey: String!`

Response: `CustomRunningNumberPagination`

---

#### getNextRunningNumber

ขอ running number ถัดไป ตาม custom running number id

ดูเลขถัดไปโดยยังไม่บันทึกการใช้

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getNextRunningNumber` (ระดับองค์กร)

```graphql
query GetNextRunningNumber($customRunningNumberId: String!) {
  getNextRunningNumber(customRunningNumberId: $customRunningNumberId)
}
```

| argument | Type |
| --- | --- |
| customRunningNumberId | `String!` |

Response: `String`

---

#### getCustomerTypes

ดึงข้อมูล customer type ทั้งหมด

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomerTypes`

```graphql
query GetCustomerTypes($getInput: GetCustomerTypeInput) {
  getCustomerTypes(getInput: $getInput) {
    customerTypes { _id organizationId organizationKey customerTypeKey customerTypeCode title }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomerTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomerTypeFilterInput` |  |  |
| search | `GetCustomerTypeSearchInput` |  |  |
| sort | `GetCustomerTypeSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `CustomerTypePagination`

---

#### getCustomerTypeByOrganizationKey

ดึงข้อมูล customer type ตาม organization key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomerTypeByOrganizationKey` (ระดับองค์กร)

```graphql
query GetCustomerTypeByOrganizationKey($organizationKey: String!, $getInput: GetCustomerTypeByOrganizationKeyInput) {
  getCustomerTypeByOrganizationKey(organizationKey: $organizationKey, getInput: $getInput) {
    customerTypes { _id organizationId organizationKey customerTypeKey customerTypeCode title }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomerTypeByOrganizationKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomerTypeByOrganizationKeyFilterInput` |  |  |
| search | `GetCustomerTypeByOrganizationKeySearchInput` |  |  |
| sort | `GetCustomerTypeSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `organizationKey: String!`

Response: `CustomerTypePagination`

---

#### getCustomerTypeById

ดึงข้อมูล customer type ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomerTypeById` (ระดับองค์กร)

```graphql
query GetCustomerTypeById($customerTypeId: String!) {
  getCustomerTypeById(customerTypeId: $customerTypeId) {
    _id
    organizationId
    organizationKey
    customerTypeKey
    customerTypeCode
    title
    subTitle
    description
    isActive
    createdBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customerTypeId | `String!` |

Response: `CustomerType`

---

#### getCustomerTypeByKey

ดึงข้อมูล customer type ตาม key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomerTypeByKey` (ระดับองค์กร)

```graphql
query GetCustomerTypeByKey($customerTypeKey: String!) {
  getCustomerTypeByKey(customerTypeKey: $customerTypeKey) {
    _id
    organizationId
    organizationKey
    customerTypeKey
    customerTypeCode
    title
    subTitle
    description
    isActive
    createdBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customerTypeKey | `String!` |

Response: `CustomerType`

---

#### getDefaultOrganizationRoles

ดึงข้อมูล defaultOrganizationRoles ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getDefaultOrganizationRoles`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetDefaultOrganizationRoles($getInput: GetDefaultOrganizationRoleInput, $appKey: String) {
  getDefaultOrganizationRoles(getInput: $getInput, appKey: $appKey) {
    defaultOrganizationRoles { _id defaultOrganizationRoleKey title subTitle description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetDefaultOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetDefaultOrganizationRoleFilterInput` |  |  |
| search | `GetDefaultOrganizationRoleSearchInput` |  |  |
| sort | `GetDefaultOrganizationRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String`

Response: `DefaultOrganizationRolePagination!`

---

#### getDefaultOrganizationRoleById

ดึงข้อมูล defaultOrganizationRole ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getDefaultOrganizationRoleById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetDefaultOrganizationRoleById($defaultOrganizationRoleId: ID!, $appKey: String) {
  getDefaultOrganizationRoleById(defaultOrganizationRoleId: $defaultOrganizationRoleId, appKey: $appKey) {
    _id
    defaultOrganizationRoleKey
    title
    subTitle
    description
    systemNote
    isActive
    createdBy
    updatedBy
    createdAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| defaultOrganizationRoleId | `ID!` |
| appKey | `String` |

Response: `DefaultOrganizationRole!`

---

#### getOrganizationApproveDetails

ดึงข้อมูล Details ของใบอนุมัติ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationApproveDetails`

```graphql
query GetOrganizationApproveDetails($organizationKey: String!, $input: GetOrganizationApproveDetailInput) {
  getOrganizationApproveDetails(organizationKey: $organizationKey, input: $input) {
    organizationApproveDetails { _id organizationId organizationKey organizationApproveId timeStamp isAuto }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationApproveDetailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationApproveDetailFilterInput` |  |  |
| search | `GetOrganizationApproveDetailSearchInput` |  |  |
| sort | `GetOrganizationApproveDetailSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `organizationKey: String!`

Response: `OrganizationApproveDetailPagination!`

---

#### getOrganizationApproveDetailById

ดึงข้อมูล Details ของใบอนุมัติตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationApproveDetailById` (ระดับองค์กร)

```graphql
query GetOrganizationApproveDetailById($organizationApproveDetailId: ID!) {
  getOrganizationApproveDetailById(organizationApproveDetailId: $organizationApproveDetailId) {
    _id
    organizationId
    organizationKey
    organizationApproveId
    timeStamp
    isAuto
    isActive
    text
    files
    images
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationApproveDetailId | `ID!` |

Response: `OrganizationApproveDetail!`

---

#### getOrganizationApproves

ดึงข้อมูล organizationApprove ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationApprove`

```graphql
query GetOrganizationApproves($input: GetOrganizationApproveInput) {
  getOrganizationApproves(input: $input) {
    organizationApproves { _id organizationId organizationKey approveStatus createdBy updatedBy }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationApproveInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationApproveFilterInput` |  |  |
| search | `GetOrganizationApproveSearchInput` |  |  |
| sort | `GetOrganizationApproveSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationApprovePagination!`

---

#### getOrganizationApproveById

ดึงข้อมูล organizationApprove ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationApproveById` (ระดับองค์กร)

```graphql
query GetOrganizationApproveById($organizationApproveId: ID!) {
  getOrganizationApproveById(organizationApproveId: $organizationApproveId) {
    _id
    organizationId
    organizationKey
    organization { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    approveStatus
    createdBy
    updatedBy
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| organizationApproveId | `ID!` |

Response: `OrganizationApprove!`

---

#### getOrganizationApproveByKey

ดึงข้อมูล organizationApprove ตาม organizationKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationApproveByKey` (ระดับองค์กร)

```graphql
query GetOrganizationApproveByKey($organizationKey: String!) {
  getOrganizationApproveByKey(organizationKey: $organizationKey) {
    _id
    organizationId
    organizationKey
    organization { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    approveStatus
    createdBy
    updatedBy
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| organizationKey | `String!` |

Response: `OrganizationApprove!`

---

#### getOrganizationContactPeoples

ดึงข้อมูล organizationContactPeoples ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactPeoples` (ระดับองค์กร)

```graphql
query GetOrganizationContactPeoples($input: GetOrganizationContactPeopleInput) {
  getOrganizationContactPeoples(input: $input) {
    organizationContactPeoples { _id organizationId organizationKey organizationPath organizationContactPeopleKey name }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationContactPeopleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationContactPeopleFilterInput` |  |  |
| search | `GetOrganizationContactPeopleSearchInput` |  |  |
| sort | `GetOrganizationContactPeopleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationContactPeoplePagination!`

---

#### getOrganizationContactPeopleById

ดึงข้อมูล organizationContactPeoples ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactPeopleById` (ระดับองค์กร)

```graphql
query GetOrganizationContactPeopleById($organizationContactPeopleId: ID!) {
  getOrganizationContactPeopleById(organizationContactPeopleId: $organizationContactPeopleId) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactPeopleKey
    name
    description
    address { address1 address2 address3 city subdivision postalCode }
    phoneNumber
    position
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationContactPeopleId | `ID!` |

Response: `OrganizationContactPeople!`

---

#### getOrganizationContactPeopleByKey

ดึงข้อมูล organizationContactPeoples ตาม organizationContactPeopleKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactPeopleByKey` (ระดับองค์กร)

```graphql
query GetOrganizationContactPeopleByKey($organizationContactPeopleKey: String!) {
  getOrganizationContactPeopleByKey(organizationContactPeopleKey: $organizationContactPeopleKey) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactPeopleKey
    name
    description
    address { address1 address2 address3 city subdivision postalCode }
    phoneNumber
    position
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationContactPeopleKey | `String!` |

Response: `OrganizationContactPeople!`

---

#### getOrganizationContactRelations

ดึงข้อมูล organizationContactRelations ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactRelations`

```graphql
query GetOrganizationContactRelations($input: GetOrganizationContactRelationInput) {
  getOrganizationContactRelations(input: $input) {
    organizationContactRelations { _id organizationId organizationKey organizationPath organizationContactRelationKey organizationContactId }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationContactRelationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationContactRelationFilterInput` |  |  |
| search | `GetOrganizationContactRelationSearchInput` |  |  |
| sort | `GetOrganizationContactRelationSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationContactRelationPagination`

---

#### getOrganizationContactRelationById

ดึงข้อมูล organizationContactRelation ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactRelationById` (ระดับองค์กร)

```graphql
query GetOrganizationContactRelationById($relationId: ID!) {
  getOrganizationContactRelationById(relationId: $relationId) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactRelationKey
    organizationContactId
    organizationContactKey
    organizationContact { _id organizationId organizationKey organizationPath organizationContactKey organizationContactCode }
    organizationContactPeopleId
    organizationContactPeopleKey
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| relationId | `ID!` |

Response: `OrganizationContactRelation`

---

#### getOrganizationContactRelationByKey

ดึงข้อมูล organizationContactRelation ตาม organizationContactRelationKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactRelationByKey` (ระดับองค์กร)

```graphql
query GetOrganizationContactRelationByKey($relationKey: String!) {
  getOrganizationContactRelationByKey(relationKey: $relationKey) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactRelationKey
    organizationContactId
    organizationContactKey
    organizationContact { _id organizationId organizationKey organizationPath organizationContactKey organizationContactCode }
    organizationContactPeopleId
    organizationContactPeopleKey
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| relationKey | `String!` |

Response: `OrganizationContactRelation`

---

#### getOrganizationContacts

ดึงข้อมูล organizationContacts ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContacts`

```graphql
query GetOrganizationContacts($input: GetOrganizationContactInput) {
  getOrganizationContacts(input: $input) {
    organizationContacts { _id organizationId organizationKey organizationPath organizationContactKey organizationContactCode }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationContactInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationContactFilterInput` |  |  |
| search | `GetOrganizationContactSearchInput` |  |  |
| sort | `GetOrganizationContactSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationContactPagination!`

---

#### getOrganizationContactById

ดึงข้อมูล organizationContacts ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactById` (ระดับองค์กร)

```graphql
query GetOrganizationContactById($organizationContactId: ID!) {
  getOrganizationContactById(organizationContactId: $organizationContactId) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactKey
    organizationContactCode
    name
    name2
    name3
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationContactId | `ID!` |

Response: `OrganizationContact!`

---

#### getOrganizationContactByKey

ดึงข้อมูล organizationContacts ตาม organizationContactKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationContactByKey` (ระดับองค์กร)

```graphql
query GetOrganizationContactByKey($organizationContactKey: String!) {
  getOrganizationContactByKey(organizationContactKey: $organizationContactKey) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactKey
    organizationContactCode
    name
    name2
    name3
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationContactKey | `String!` |

Response: `OrganizationContact!`

---

#### getOrganizationContactByTaxID

ดึงข้อมูล organizationContacts ตาม taxIdentificationNumber

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationContactByTaxID($organizationKey: String!, $taxIdentificationNumber: String!) {
  getOrganizationContactByTaxID(organizationKey: $organizationKey, taxIdentificationNumber: $taxIdentificationNumber) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactKey
    organizationContactCode
    name
    name2
    name3
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationKey | `String!` |
| taxIdentificationNumber | `String!` |

Response: `OrganizationContact`

---

#### getOrganizationRoles

ดึงข้อมูล organizationRoles ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRoles`

```graphql
query GetOrganizationRoles($input: GetOrganizationRoleInput) {
  getOrganizationRoles(input: $input) {
    organizationRoles { _id organizationId organizationKey organizationRoleKey title subTitle }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationRoleFilterInput` |  |  |
| search | `GetOrganizationRoleSearchInput` |  |  |
| sort | `GetOrganizationRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationRolePagination!`

---

#### getOrganizationRolesByOrganizationKey

ดึงข้อมูล organizationRoles ตาม organizationKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRolesByOrganizationKey` (ระดับองค์กร)

```graphql
query GetOrganizationRolesByOrganizationKey($organizationKey: String!, $input: GetOrganizationRoleInput) {
  getOrganizationRolesByOrganizationKey(organizationKey: $organizationKey, input: $input) {
    organizationRoles { _id organizationId organizationKey organizationRoleKey title subTitle }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationRoleFilterInput` |  |  |
| search | `GetOrganizationRoleSearchInput` |  |  |
| sort | `GetOrganizationRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `organizationKey: String!`

Response: `OrganizationRolePagination!`

---

#### getOrganizationRoleById

ดึงข้อมูล organizationRoles ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRoleById` (ระดับองค์กร)

```graphql
query GetOrganizationRoleById($organizationRoleId: ID!) {
  getOrganizationRoleById(organizationRoleId: $organizationRoleId) {
    _id
    organizationId
    organizationKey
    organizationRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationRoleId | `ID!` |

Response: `OrganizationRole!`

---

#### getOrganizationRoleByKey

ดึงข้อมูล organizationRoles ตาม organizationRoleKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRoleByKey` (ระดับองค์กร)

```graphql
query GetOrganizationRoleByKey($organizationRoleKey: String!, $organizationKey: String!) {
  getOrganizationRoleByKey(organizationRoleKey: $organizationRoleKey, organizationKey: $organizationKey) {
    _id
    organizationId
    organizationKey
    organizationRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationRoleKey | `String!` |
| organizationKey | `String!` |

Response: `OrganizationRole!`

---

#### getOrganizationTags

ดึงข้อมูล organizationTags ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationTags($input: GetOrganizationTagInput) {
  getOrganizationTags(input: $input) {
    organizationTags { _id organizationTagKey organizationTagPath parentOrganizationTagId title subTitle }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationTagInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationTagFilterInput` |  |  |
| search | `GetOrganizationTagSearchInput` |  |  |
| sort | `GetOrganizationTagSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationTagPagination!`

---

#### getOrganizationTagById

ดึงข้อมูล organizationTags ตาม ID

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationTagById($organizationTagId: ID!) {
  getOrganizationTagById(organizationTagId: $organizationTagId) {
    _id
    organizationTagKey
    organizationTagPath
    parentOrganizationTagId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationTagId | `ID!` |

Response: `OrganizationTag!`

---

#### getOrganizationTagByKey

ดึงข้อมูล organizationTags ตาม

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationTagByKey($organizationTagKey: String!) {
  getOrganizationTagByKey(organizationTagKey: $organizationTagKey) {
    _id
    organizationTagKey
    organizationTagPath
    parentOrganizationTagId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationTagKey | `String!` |

Response: `OrganizationTag!`

---

#### getOrganizationTypes

ดึงข้อมูล organizationTypes ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationTypes($input: GetOrganizationTypeInput) {
  getOrganizationTypes(input: $input) {
    organizationTypes { _id organizationTypeKey organizationTypePath parentOrganizationTypeId title subTitle }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationTypeFilterInput` |  |  |
| search | `GetOrganizationTypeSearchInput` |  |  |
| sort | `GetOrganizationTypeSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationTypePagination!`

---

#### getOrganizationTypeById

ดึงข้อมูล organizationTypes ตาม ID

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationTypeById($organizationTypeId: ID!) {
  getOrganizationTypeById(organizationTypeId: $organizationTypeId) {
    _id
    organizationTypeKey
    organizationTypePath
    parentOrganizationTypeId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationTypeId | `ID!` |

Response: `OrganizationType!`

---

#### getOrganizationTypeByKey

ดึงข้อมูล organizationTypes ตาม

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetOrganizationTypeByKey($organizationTypeKey: String!) {
  getOrganizationTypeByKey(organizationTypeKey: $organizationTypeKey) {
    _id
    organizationTypeKey
    organizationTypePath
    parentOrganizationTypeId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationTypeKey | `String!` |

Response: `OrganizationType!`

---

#### getOrganizations

ดึงข้อมูล organizations

ปกติเห็นเฉพาะองค์กรที่ผู้ใช้มีสิทธิ์ · ผู้ที่มีสิทธิ์ `queryAllOrganizations` เห็นทุกองค์กรในแอป

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizations` (ระดับองค์กร)

```graphql
query GetOrganizations($input: GetOrganizationInput) {
  getOrganizations(input: $input) {
    organizations { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationFilterInput` |  |  |
| search | `GetOrganizationSearchInput` |  |  |
| sort | `GetOrganizationSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationPagination!`

---

#### getOrganizationById

ดึงข้อมูล organizations ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationById` (ระดับองค์กร)

```graphql
query GetOrganizationById($organizationId: ID!) {
  getOrganizationById(organizationId: $organizationId) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationId | `ID!` |

Response: `Organization!`

---

#### getOrganizationByKey

ดึงข้อมูล organizations ตาม organizationKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationByKey` (ระดับองค์กร)

```graphql
query GetOrganizationByKey($organizationKey: String!) {
  getOrganizationByKey(organizationKey: $organizationKey) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationKey | `String!` |

Response: `Organization!`

---

#### getPublicOrganizations

ดึงข้อมูล organizations ที่ isPublic = true AND isApproved = true AND isActive = true

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPublicOrganizations($input: GetOrganizationInput) {
  getPublicOrganizations(input: $input) {
    organizations { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrganizationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrganizationFilterInput` |  |  |
| search | `GetOrganizationSearchInput` |  |  |
| sort | `GetOrganizationSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrganizationPagination!`

---

#### getPublicOrganizationById

ดึงข้อมูล organizations ตาม ID ที่ isPublic = true AND isApproved = true AND isActive = true

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPublicOrganizationById($organizationId: ID!) {
  getPublicOrganizationById(organizationId: $organizationId) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationId | `ID!` |

Response: `Organization!`

---

#### getPublicOrganizationByKey

ดึงข้อมูล organizations ตาม organizationKey ที่ isPublic = true AND isApproved = true AND isActive = true

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPublicOrganizationByKey($organizationKey: String!) {
  getPublicOrganizationByKey(organizationKey: $organizationKey) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationKey | `String!` |

Response: `Organization!`

---

#### getUserOrganizations

ดึงข้อมูล userOrganizations ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserOrganizations`

```graphql
query GetUserOrganizations($getInput: GetUserOrganizationsInput!) {
  getUserOrganizations(getInput: $getInput) {
    userOrganizations { _id authId organizationId organizationKey organizationPath }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetUserOrganizationsInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `UserOrganizationFilterInput` |  |  |
| search | `UserOrganizationSearchInput` |  |  |
| sort | `UserOrganizationSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `UserOrganizationPagination`

---

#### getUserOrganizationById

ดึงข้อมูล userOrganization โดย id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserOrganization`

```graphql
query GetUserOrganizationById($id: String!) {
  getUserOrganizationById(id: $id) {
    _id
    authId
    organizationId
    organizationKey
    organizationPath
  }
}
```

| argument | Type |
| --- | --- |
| id | `String!` |

Response: `UserOrganization`

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

---

#### createAppRole

สร้างข้อมูล AppRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createAppRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation CreateAppRole($createInput: CreateAppRoleInput!) {
  createAppRole(createInput: $createInput) {
    _id
    appRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    createdBy
    updatedBy
    # ...
  }
}
```

`createInput`: `CreateAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  | สามารถสร้าง ข้าม app ได้โดยการระบุ appkey ที่อยากสร้างลงไป |
| appRoleKey | `String` |  |  |
| title | `String!` | ใช่ |  |
| subTitle | `String` |  |  |
| description | `String` |  |  |
| systemNote | `String` |  |  |
| isInvite | `Boolean` |  |  |
| isActive | `Boolean` |  |  |

Response: `AppRole!`

---

#### updateAppRole

แก้ไขข้อมูล AppRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateAppRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateAppRole($appRoleId: ID!, $updateInput: UpdateAppRoleInput!) {
  updateAppRole(appRoleId: $appRoleId, updateInput: $updateInput) {
    _id
    appRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    createdBy
    updatedBy
    # ...
  }
}
```

`updateInput`: `UpdateAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| title | `String` |  |  |
| subTitle | `String` |  |  |
| description | `String` |  |  |
| systemNote | `String` |  |  |
| isInvite | `Boolean` |  |  |
| isActive | `Boolean` |  |  |

argument อื่น: `appRoleId: ID!`

Response: `AppRole!`

---

#### deleteAppRole

ลบข้อมูล AppRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteAppRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation DeleteAppRole($appRoleId: ID!) {
  deleteAppRole(appRoleId: $appRoleId) {
    _id
    appRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRoleId | `ID!` |

Response: `AppRole!`

---

#### deleteCustomRunningNumberLog

ลบ custom running number log ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteCustomRunningNumberLog` (ระดับองค์กร)

```graphql
mutation DeleteCustomRunningNumberLog($customRunningNumberLogId: String!) {
  deleteCustomRunningNumberLog(customRunningNumberLogId: $customRunningNumberLogId) {
    _id
    organizationId
    organizationKey
    customRunningNumberId
    customRunningNumberKey
    generatedCode
    createdBy
    updatedBy
    createdAt
    updatedAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customRunningNumberLogId | `String!` |

Response: `CustomRunningNumberLog`

---

#### createCustomRunningNumber

สร้าง custom running number

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createCustomRunningNumber` (ระดับองค์กร)

```graphql
mutation CreateCustomRunningNumber($createInput: CreateCustomRunningNumberInput!) {
  createCustomRunningNumber(createInput: $createInput) {
    _id
    level
    organizationId
    organizationKey
    customRunningNumberKey
    refKey
    order
    title
    description
    serial
    # ...
  }
}
```

`createInput`: `CreateCustomRunningNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| level | `EnumLevel` |  | level |
| organizationKey | `String` |  | key ของ org ถ้าเป็น org level ต้องระบุ app level ไม่ต้องระบุ |
| customRunningNumberKey | `String` |  | key ของ custom running number |
| refKey | `String!` | ใช่ | ref key |
| order | `Float` |  | ลำดับ |
| title | `String` |  | ชื่อ |
| description | `String` |  | คำอธิบาย |
| serial | `EnumSerialType` |  | ประเภท serial |
| padding | `Int` |  | padding |
| pattern | `String!` | ใช่ | pattern |
| isDefault | `Boolean` |  | เป็น default หรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน |

Response: `CustomRunningNumber!`

---

#### updateCustomRunningNumber

แก้ไข custom running number ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateCustomRunningNumber` (ระดับองค์กร)

```graphql
mutation UpdateCustomRunningNumber($customRunningNumberId: String!, $updateInput: UpdateCustomRunningNumberInput!) {
  updateCustomRunningNumber(customRunningNumberId: $customRunningNumberId, updateInput: $updateInput) {
    _id
    level
    organizationId
    organizationKey
    customRunningNumberKey
    refKey
    order
    title
    description
    serial
    # ...
  }
}
```

`updateInput`: `UpdateCustomRunningNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| order | `Float` |  | ลำดับ |
| title | `String` |  | ชื่อ |
| description | `String` |  | คำอธิบาย |
| serial | `EnumSerialType` |  | ประเภท serial |
| padding | `Int` |  | padding |
| pattern | `String` |  | pattern |
| isDefault | `Boolean` |  | เป็น default หรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน ถ้าถูกใช้อยู่จะไม่สามารถเปลี่ยนแปลงได้ |

argument อื่น: `customRunningNumberId: String!`

Response: `CustomRunningNumber!`

---

#### deleteCustomRunningNumber

ลบ custom running number ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteCustomRunningNumber` (ระดับองค์กร)

```graphql
mutation DeleteCustomRunningNumber($customRunningNumberId: String!) {
  deleteCustomRunningNumber(customRunningNumberId: $customRunningNumberId) {
    _id
    level
    organizationId
    organizationKey
    customRunningNumberKey
    refKey
    order
    title
    description
    serial
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customRunningNumberId | `String!` |

Response: `CustomRunningNumber`

---

#### useCustomRunningNumber

ใช้ custom running number ตาม id

ออกเลขถัดไปและบันทึก log ของการใช้เลข

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `useCustomRunningNumber` (ระดับองค์กร)

```graphql
mutation UseCustomRunningNumber($useInput: UseCustomRunningNumberInput!) {
  useCustomRunningNumber(useInput: $useInput) {
    _id
    organizationId
    organizationKey
    customRunningNumberId
    customRunningNumberKey
    generatedCode
    createdBy
    updatedBy
    createdAt
    updatedAt
    # ...
  }
}
```

`useInput`: `UseCustomRunningNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| customRunningNumberId | `String!` | ใช่ | key ของ custom running number |
| generatedCode | `String!` | ใช่ | code ที่ใช้ |

Response: `CustomRunningNumberLog`

---

#### createCustomerType

สร้าง customer type

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createCustomerType` (ระดับองค์กร)

```graphql
mutation CreateCustomerType($createInput: CreateCustomerTypeInput!) {
  createCustomerType(createInput: $createInput) {
    _id
    organizationId
    organizationKey
    customerTypeKey
    customerTypeCode
    title
    subTitle
    description
    isActive
    createdBy
    # ...
  }
}
```

`createInput`: `CreateCustomerTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String!` | ใช่ | key ของ org |
| customerTypeKey | `String` |  | key ของ customer type |
| customerTypeCode | `String` |  | รหัสของ customer type |
| title | `String!` | ใช่ | ชื่อของ customer type |
| subTitle | `String` |  | ชื่อย่อของ customer type |
| description | `String` |  | คำอธิบายของ customer type |
| isActive | `Boolean` |  | สถานะการใช้งาน |

Response: `CustomerType!`

---

#### updateCustomerType

แก้ไข customer type ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateCustomerType` (ระดับองค์กร)

```graphql
mutation UpdateCustomerType($customerTypeId: String!, $updateInput: UpdateCustomerTypeInput!) {
  updateCustomerType(customerTypeId: $customerTypeId, updateInput: $updateInput) {
    _id
    organizationId
    organizationKey
    customerTypeKey
    customerTypeCode
    title
    subTitle
    description
    isActive
    createdBy
    # ...
  }
}
```

`updateInput`: `UpdateCustomerTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| customerTypeCode | `String` |  | รหัสของ customer type |
| title | `String` |  | ชื่อของ customer type |
| subTitle | `String` |  | ชื่อย่อของ customer type |
| description | `String` |  | คำอธิบายของ customer type |
| isActive | `Boolean` |  | สถานะการใช้งาน ถ้าถูกใช้อยู่จะไม่สามารถเปลี่ยนแปลงได้ |

argument อื่น: `customerTypeId: String!`

Response: `CustomerType!`

---

#### deleteCustomerType

ลบ customer type ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteCustomerType` (ระดับองค์กร)

```graphql
mutation DeleteCustomerType($customerTypeId: String!) {
  deleteCustomerType(customerTypeId: $customerTypeId) {
    _id
    organizationId
    organizationKey
    customerTypeKey
    customerTypeCode
    title
    subTitle
    description
    isActive
    createdBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customerTypeId | `String!` |

Response: `CustomerType`

---

#### createDefaultOrganizationRole

สร้างข้อมูล DefaultOrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation CreateDefaultOrganizationRole($createInput: CreateDefaultOrganizationRoleInput!) {
  createDefaultOrganizationRole(createInput: $createInput) {
    _id
    defaultOrganizationRoleKey
    title
    subTitle
    description
    systemNote
    isActive
    createdBy
    updatedBy
    createdAt
    # ...
  }
}
```

`createInput`: `CreateDefaultOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  | appKey แอพพลิเคชั่น (optional, จะใช้ของ user ถ้าไม่ระบุ) |
| defaultOrganizationRoleKey | `String` |  | defaultOrganizationRoleKey รหัส default organization role |
| title | `String!` | ใช่ | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `DefaultOrganizationRole!`

---

#### updateDefaultOrganizationRole

แก้ไขข้อมูล DefaultOrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateDefaultOrganizationRole($defaultOrganizationRoleId: ID!, $updateInput: UpdateDefaultOrganizationRoleInput!) {
  updateDefaultOrganizationRole(defaultOrganizationRoleId: $defaultOrganizationRoleId, updateInput: $updateInput) {
    _id
    defaultOrganizationRoleKey
    title
    subTitle
    description
    systemNote
    isActive
    createdBy
    updatedBy
    createdAt
    # ...
  }
}
```

`updateInput`: `UpdateDefaultOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| title | `String` |  | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `defaultOrganizationRoleId: ID!`

Response: `DefaultOrganizationRole!`

---

#### deleteDefaultOrganizationRole

ลบข้อมูล DefaultOrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation DeleteDefaultOrganizationRole($defaultOrganizationRoleId: ID!) {
  deleteDefaultOrganizationRole(defaultOrganizationRoleId: $defaultOrganizationRoleId) {
    _id
    defaultOrganizationRoleKey
    title
    subTitle
    description
    systemNote
    isActive
    createdBy
    updatedBy
    createdAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| defaultOrganizationRoleId | `ID!` |

Response: `DefaultOrganizationRole!`

---

#### createOrganizationApproveDetail

สร้างข้อมูล OrganizationApproveDetail

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationApproveDetail` (ระดับองค์กร)

```graphql
mutation CreateOrganizationApproveDetail($createInput: CreateOrganizationApproveDetailInput!) {
  createOrganizationApproveDetail(createInput: $createInput) {
    _id
    organizationId
    organizationKey
    organizationApproveId
    timeStamp
    isAuto
    isActive
    text
    files
    images
    # ...
  }
}
```

`createInput`: `CreateOrganizationApproveDetailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String!` | ใช่ | organizationKey ของ organization ที่ระบุ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| text | `String!` | ใช่ | ข้อความแสดงผล |
| files | `[String]` |  | array files |
| images | `[String]` |  | array images |

Response: `OrganizationApproveDetail!`

---

#### updateOrganizationApproveDetail

แก้ไขข้อมูล OrganizationApproveDetail

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationApproveDetail` (ระดับองค์กร)

```graphql
mutation UpdateOrganizationApproveDetail($organizationApproveDetailId: ID!, $updateInput: UpdateOrganizationApproveDetailInput!) {
  updateOrganizationApproveDetail(organizationApproveDetailId: $organizationApproveDetailId, updateInput: $updateInput) {
    _id
    organizationId
    organizationKey
    organizationApproveId
    timeStamp
    isAuto
    isActive
    text
    files
    images
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationApproveDetailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| text | `String` |  | ข้อความแสดงผล |
| files | `[String]` |  | array files |
| images | `[String]` |  | array images |

argument อื่น: `organizationApproveDetailId: ID!`

Response: `OrganizationApproveDetail!`

---

#### deleteOrganizationApproveDetail

ลบข้อมูล OrganizationApproveDetail

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationApproveDetail` (ระดับองค์กร)

```graphql
mutation DeleteOrganizationApproveDetail($organizationApproveDetailId: ID!) {
  deleteOrganizationApproveDetail(organizationApproveDetailId: $organizationApproveDetailId) {
    _id
    organizationId
    organizationKey
    organizationApproveId
    timeStamp
    isAuto
    isActive
    text
    files
    images
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationApproveDetailId | `ID!` |

Response: `OrganizationApproveDetail!`

---

#### setOrganizationApproveStatusToInProgress

ใช้เพื่อเปลี่ยนสถานะใบอนุมัติเป็น IN_PROGRESS

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `setOrganizationApproveStatusToInProgress` (ระดับองค์กร)

```graphql
mutation SetOrganizationApproveStatusToInProgress($input: SetOrganizationApproveStatusInput!) {
  setOrganizationApproveStatusToInProgress(input: $input) {
    _id
    organizationId
    organizationKey
    organization { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    approveStatus
    createdBy
    updatedBy
    createdAt
    updatedAt
  }
}
```

`input`: `SetOrganizationApproveStatusInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String!` | ใช่ | organizationKey ที่ระบุ |
| text | `String` |  | ข้อความที่ใช้ระบุเหตุผลการดำเนินการ |

Response: `OrganizationApprove!`

---

#### setOrganizationApproveStatusToApproved

ใช้เพื่อเปลี่ยนสถานะใบอนุมัติเป็น APPROVED

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `setOrganizationApproveStatusToApproved` (ระดับองค์กร)

```graphql
mutation SetOrganizationApproveStatusToApproved($input: SetOrganizationApproveStatusInput!) {
  setOrganizationApproveStatusToApproved(input: $input) {
    _id
    organizationId
    organizationKey
    organization { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    approveStatus
    createdBy
    updatedBy
    createdAt
    updatedAt
  }
}
```

`input`: `SetOrganizationApproveStatusInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String!` | ใช่ | organizationKey ที่ระบุ |
| text | `String` |  | ข้อความที่ใช้ระบุเหตุผลการดำเนินการ |

Response: `OrganizationApprove!`

---

#### setOrganizationApproveStatusToRejected

ใช้เพื่อเปลี่ยนสถานะใบอนุมัติเป็น REJECTED

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `setOrganizationApproveStatusToRejected` (ระดับองค์กร)

```graphql
mutation SetOrganizationApproveStatusToRejected($input: SetOrganizationApproveStatusInput!) {
  setOrganizationApproveStatusToRejected(input: $input) {
    _id
    organizationId
    organizationKey
    organization { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    approveStatus
    createdBy
    updatedBy
    createdAt
    updatedAt
  }
}
```

`input`: `SetOrganizationApproveStatusInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String!` | ใช่ | organizationKey ที่ระบุ |
| text | `String` |  | ข้อความที่ใช้ระบุเหตุผลการดำเนินการ |

Response: `OrganizationApprove!`

---

#### setOrganizationApproveStatusToNeedMoreInformation

ใช้เพื่อเปลี่ยนสถานะใบอนุมัติเป็น NEED_MORE_INFORMATION

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `setOrganizationApproveStatusToNeedMoreInformation` (ระดับองค์กร)

```graphql
mutation SetOrganizationApproveStatusToNeedMoreInformation($input: SetOrganizationApproveStatusInput!) {
  setOrganizationApproveStatusToNeedMoreInformation(input: $input) {
    _id
    organizationId
    organizationKey
    organization { _id organizationKey organizationPath parentOrganizationId organizationImageKey organizationBackgroundImageKey }
    approveStatus
    createdBy
    updatedBy
    createdAt
    updatedAt
  }
}
```

`input`: `SetOrganizationApproveStatusInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String!` | ใช่ | organizationKey ที่ระบุ |
| text | `String` |  | ข้อความที่ใช้ระบุเหตุผลการดำเนินการ |

Response: `OrganizationApprove!`

---

#### createOrganizationContactPeople

สร้างข้อมูล OrganizationContactPeople

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationContactPeople` (ระดับองค์กร)

```graphql
mutation CreateOrganizationContactPeople($createInput: CreateOrganizationContactPeopleInput!) {
  createOrganizationContactPeople(createInput: $createInput) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactPeopleKey
    name
    description
    address { address1 address2 address3 city subdivision postalCode }
    phoneNumber
    position
    # ...
  }
}
```

`createInput`: `CreateOrganizationContactPeopleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationContactPeopleKey | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| organizationKey | `String!` | ใช่ | organizationKey organization ที่ระบุ |
| organizationContactKeys | `[String]` |  | organizationContactKey เพื่อสร้าง relation อัตโนมัต |
| name | `String!` | ใช่ | ชื่อ |
| description | `String` |  | คำอธิบาย |
| address | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| phoneNumber | `String` |  | phoneNumber text |
| position | `String` |  | position text |
| email | `String` |  | email |
| taxIdentificationNumber | `String` |  | taxIdentificationNumber |
| taxIdentificationExpireDate | `Date` |  | วันหมดอายุของเลขประจำตัวผู้เสียภาษี |
| systemNote | `String` |  | ไว้สำหรับ admin |
| documentKeys | `[String]` |  | file key เอกสาร |
| profileImageKey | `String` |  | profileImageKey เป็น key ของรูปโปรไฟล์ที่เก็บใน storage |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `OrganizationContactPeople!`

---

#### updateOrganizationContactPeople

แก้ไขข้อมูล OrganizationContactPeople

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationContactPeople` (ระดับองค์กร)

```graphql
mutation UpdateOrganizationContactPeople($organizationContactPeopleId: ID!, $updateInput: UpdateOrganizationContactPeopleInput!) {
  updateOrganizationContactPeople(organizationContactPeopleId: $organizationContactPeopleId, updateInput: $updateInput) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactPeopleKey
    name
    description
    address { address1 address2 address3 city subdivision postalCode }
    phoneNumber
    position
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationContactPeopleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| name | `String` |  | ชื่อ |
| description | `String` |  | คำอธิบาย |
| address | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| phoneNumber | `String` |  | phoneNumber text |
| position | `String` |  | position text |
| email | `String` |  | email |
| taxIdentificationNumber | `String` |  | taxIdentificationNumber |
| taxIdentificationExpireDate | `Date` |  | วันหมดอายุของเลขประจำตัวผู้เสียภาษี |
| systemNote | `String` |  | ไว้สำหรับ admin |
| documentKeys | `[String]` |  | file key เอกสาร |
| profileImageKey | `String` |  | profileImageKey เป็น key ของรูปโปรไฟล์ที่เก็บใน storage |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `organizationContactPeopleId: ID!`

Response: `OrganizationContactPeople!`

---

#### deleteOrganizationContactPeople

ลบข้อมูล OrganizationContactPeople

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationContactPeople` (ระดับองค์กร)

```graphql
mutation DeleteOrganizationContactPeople($organizationContactPeopleId: ID!) {
  deleteOrganizationContactPeople(organizationContactPeopleId: $organizationContactPeopleId) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactPeopleKey
    name
    description
    address { address1 address2 address3 city subdivision postalCode }
    phoneNumber
    position
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationContactPeopleId | `ID!` |

Response: `OrganizationContactPeople!`

---

#### createOrganizationContactRelation

สร้างข้อมูล OrganizationContactRelation

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationContactRelation` (ระดับองค์กร)

```graphql
mutation CreateOrganizationContactRelation($createInput: CreateOrganizationContactRelationInput!) {
  createOrganizationContactRelation(createInput: $createInput) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactRelationKey
    organizationContactId
    organizationContactKey
    organizationContact { _id organizationId organizationKey organizationPath organizationContactKey organizationContactCode }
    organizationContactPeopleId
    organizationContactPeopleKey
    # ...
  }
}
```

`createInput`: `CreateOrganizationContactRelationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationContactKey | `String!` | ใช่ | organizationContactKey |
| organizationContactPeopleKey | `String!` | ใช่ | organizationContactPeopleKey |
| organizationContactRelationKey | `String` |  | organizationContactRelationKey autogen ใช้เพื่อแสดงหน้าบ้าน |
| order | `Float` |  | ลำดับการแสดงผล |
| systemNote | `String` |  | โน๊ต |
| position | `String` |  | ต่ำแหน่งของผู้ติดต่อ |
| documentKeys | `[String]` |  | file key เอกสาร |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `OrganizationContactRelation`

---

#### updateOrganizationContactRelation

แก้ไขข้อมูล OrganizationContactRelation

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationContactRelation` (ระดับองค์กร)

```graphql
mutation UpdateOrganizationContactRelation($relationId: ID!, $updateInput: UpdateOrganizationContactRelationInput!) {
  updateOrganizationContactRelation(relationId: $relationId, updateInput: $updateInput) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactRelationKey
    organizationContactId
    organizationContactKey
    organizationContact { _id organizationId organizationKey organizationPath organizationContactKey organizationContactCode }
    organizationContactPeopleId
    organizationContactPeopleKey
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationContactRelationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| order | `Float` |  | ลำดับการแสดงผล |
| systemNote | `String` |  | โน๊ต |
| position | `String` |  | ต่ำแหน่งของผู้ติดต่อ |
| documentKeys | `[String]` |  | file key เอกสาร |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `relationId: ID!`

Response: `OrganizationContactRelation`

---

#### deleteOrganizationContactRelation

ลบข้อมูล OrganizationContactRelation

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationContactRelation` (ระดับองค์กร)

```graphql
mutation DeleteOrganizationContactRelation($relationId: ID!) {
  deleteOrganizationContactRelation(relationId: $relationId) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactRelationKey
    organizationContactId
    organizationContactKey
    organizationContact { _id organizationId organizationKey organizationPath organizationContactKey organizationContactCode }
    organizationContactPeopleId
    organizationContactPeopleKey
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| relationId | `ID!` |

Response: `OrganizationContactRelation`

---

#### createOrganizationContact

สร้างข้อมูล OrganizationContact

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationContact` (ระดับองค์กร)

```graphql
mutation CreateOrganizationContact($createInput: CreateOrganizationContactInput!) {
  createOrganizationContact(createInput: $createInput) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactKey
    organizationContactCode
    name
    name2
    name3
    description
    # ...
  }
}
```

`createInput`: `CreateOrganizationContactInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationContactKey | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| organizationContactCode | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| organizationKey | `String!` | ใช่ | organizationKey organization ที่ระบุ |
| name | `String!` | ใช่ | ชื่อ |
| name2 | `String` |  | ชื่อ 2 |
| name3 | `String` |  | ชื่อ 3 |
| description | `String` |  | คำอธิบาย |
| type | `EnumOrganizationContactType` |  | ประเภทของบริษัท เช่น บุคคลธรรมดา, นิติบุคคล, อื่นๆ |
| customerTypeKey | `String` |  | key ประเภทของลูกค้า |
| address | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| address2 | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| phoneNumber | `String` |  | phoneNumber text |
| position | `String` |  | position text |
| email | `String` |  | email |
| taxIdentificationNumber | `String` |  | taxIdentificationNumber |
| systemNote | `String` |  | ไว้สำหรับ admin |
| documentKeys | `[String]` |  | file key เอกสาร |
| isCustomer | `Boolean` |  | เป็น customer หรือไม่ |
| isSupplier | `Boolean` |  | เป็น supplier หรือไม่ |
| directors | `[DirectorInput]` |  | ข้อมูลกรรมการ |
| documentFile | `DocumentFileInput` |  | ข้อมูลไฟล์เอกสาร |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| … | | | (มีอีก 1 ฟิลด์ ดู schema) |

Response: `OrganizationContact!`

---

#### updateOrganizationContact

แก้ไขข้อมูล OrganizationContact

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationContact` (ระดับองค์กร)

```graphql
mutation UpdateOrganizationContact($organizationContactId: ID!, $updateInput: UpdateOrganizationContactInput!) {
  updateOrganizationContact(organizationContactId: $organizationContactId, updateInput: $updateInput) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactKey
    organizationContactCode
    name
    name2
    name3
    description
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationContactInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationContactCode | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| name | `String` |  | ชื่อ |
| name2 | `String` |  | ชื่อ 2 |
| name3 | `String` |  | ชื่อ 3 |
| description | `String` |  | คำอธิบาย |
| type | `EnumOrganizationContactType` |  | ประเภทของบริษัท เช่น บุคคลธรรมดา, นิติบุคคล, อื่นๆ |
| customerTypeKey | `String` |  | key ประเภทของลูกค้า null = ลบออก |
| address | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| address2 | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| phoneNumber | `String` |  | phoneNumber text |
| position | `String` |  | position text |
| email | `String` |  | email |
| taxIdentificationNumber | `String` |  | taxIdentificationNumber |
| systemNote | `String` |  | ไว้สำหรับ admin |
| documentKeys | `[String]` |  | file key เอกสาร |
| isCustomer | `Boolean` |  | เป็น customer หรือไม่ |
| isSupplier | `Boolean` |  | เป็น supplier หรือไม่ |
| directors | `[DirectorInput]` |  | ข้อมูลกรรมการ |
| documentFile | `DocumentFileInput` |  | ข้อมูลไฟล์เอกสาร |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `organizationContactId: ID!`

Response: `OrganizationContact!`

---

#### deleteOrganizationContact

ลบข้อมูล OrganizationContact

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationContact` (ระดับองค์กร)

```graphql
mutation DeleteOrganizationContact($organizationContactId: ID!) {
  deleteOrganizationContact(organizationContactId: $organizationContactId) {
    _id
    organizationId
    organizationKey
    organizationPath
    organizationContactKey
    organizationContactCode
    name
    name2
    name3
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationContactId | `ID!` |

Response: `OrganizationContact!`

---

#### createOrganizationRole

สร้างข้อมูล OrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationRole` (ระดับองค์กร)

```graphql
mutation CreateOrganizationRole($createInput: CreateOrganizationRoleInput!) {
  createOrganizationRole(createInput: $createInput) {
    _id
    organizationId
    organizationKey
    organizationRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    # ...
  }
}
```

`createInput`: `CreateOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationRoleKey | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| organizationKey | `String!` | ใช่ | organizationKey organization ที่ระบุ |
| title | `String!` | ใช่ | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isInvite | `Boolean` |  | นำไปใช้ใน inviteCode ได้ไหม |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `OrganizationRole!`

---

#### updateOrganizationRole

แก้ไขข้อมูล OrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationRole` (ระดับองค์กร)

```graphql
mutation UpdateOrganizationRole($organizationRoleId: ID!, $updateInput: UpdateOrganizationRoleInput!) {
  updateOrganizationRole(organizationRoleId: $organizationRoleId, updateInput: $updateInput) {
    _id
    organizationId
    organizationKey
    organizationRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| title | `String` |  | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isInvite | `Boolean` |  | นำไปใช้ใน inviteCode ได้ไหม |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `organizationRoleId: ID!`

Response: `OrganizationRole!`

---

#### deleteOrganizationRole

ลบข้อมูล OrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationRole` (ระดับองค์กร)

```graphql
mutation DeleteOrganizationRole($organizationRoleId: ID!) {
  deleteOrganizationRole(organizationRoleId: $organizationRoleId) {
    _id
    organizationId
    organizationKey
    organizationRoleKey
    title
    subTitle
    description
    systemNote
    isInvite
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationRoleId | `ID!` |

Response: `OrganizationRole!`

---

#### createOrganizationTag

สร้างข้อมูล OrganizationTag

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationTag`

```graphql
mutation CreateOrganizationTag($createInput: CreateOrganizationTagInput!) {
  createOrganizationTag(createInput: $createInput) {
    _id
    organizationTagKey
    organizationTagPath
    parentOrganizationTagId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

`createInput`: `CreateOrganizationTagInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationTagKey | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| parentOrganizationTagId | `String` |  | _id organizationTag ของแม่ |
| title | `String!` | ใช่ | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| icon | `String` |  | icon |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isPublic | `Boolean` |  | สถานะการPublic true เมื่อเปิดให้ Public |

Response: `OrganizationTag!`

---

#### updateOrganizationTag

แก้ไขข้อมูล OrganizationTag ถ้ามีลูก ห้ามแก้ organizationParent

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationTag`

```graphql
mutation UpdateOrganizationTag($organizationTagId: ID!, $updateInput: UpdateOrganizationTagInput!) {
  updateOrganizationTag(organizationTagId: $organizationTagId, updateInput: $updateInput) {
    _id
    organizationTagKey
    organizationTagPath
    parentOrganizationTagId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationTagInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| parentOrganizationTagId | `String` |  | _id organizationTag ของแม่ |
| title | `String` |  | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| icon | `String` |  | icon |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isPublic | `Boolean` |  | สถานะการPublic true เมื่อเปิดให้ Public |

argument อื่น: `organizationTagId: ID!`

Response: `OrganizationTag!`

---

#### deleteOrganizationTag

ลบข้อมูล OrganizationTag ถ้ามีลูก ห้ามลบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationTag`

```graphql
mutation DeleteOrganizationTag($organizationTagId: ID!) {
  deleteOrganizationTag(organizationTagId: $organizationTagId) {
    _id
    organizationTagKey
    organizationTagPath
    parentOrganizationTagId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationTagId | `ID!` |

Response: `OrganizationTag!`

---

#### createOrganizationType

สร้างข้อมูล OrganizationType

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganizationType`

```graphql
mutation CreateOrganizationType($createInput: CreateOrganizationTypeInput!) {
  createOrganizationType(createInput: $createInput) {
    _id
    organizationTypeKey
    organizationTypePath
    parentOrganizationTypeId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

`createInput`: `CreateOrganizationTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationTypeKey | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| parentOrganizationTypeId | `String` |  | _id organizationType ของแม่ |
| title | `String!` | ใช่ | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| icon | `String` |  | icon |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isPublic | `Boolean` |  | สถานะการPublic true เมื่อเปิดให้ Public |

Response: `OrganizationType!`

---

#### updateOrganizationType

แก้ไขข้อมูล OrganizationType ถ้ามีลูก ห้ามแก้ organizationParent

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationType`

```graphql
mutation UpdateOrganizationType($organizationTypeId: ID!, $updateInput: UpdateOrganizationTypeInput!) {
  updateOrganizationType(organizationTypeId: $organizationTypeId, updateInput: $updateInput) {
    _id
    organizationTypeKey
    organizationTypePath
    parentOrganizationTypeId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| parentOrganizationTypeId | `String` |  | _id organizationType ของแม่ |
| title | `String` |  | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| icon | `String` |  | icon |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isPublic | `Boolean` |  | สถานะการPublic true เมื่อเปิดให้ Public |

argument อื่น: `organizationTypeId: ID!`

Response: `OrganizationType!`

---

#### deleteOrganizationType

ลบข้อมูล OrganizationType ถ้ามีลูก ห้ามลบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganizationType`

```graphql
mutation DeleteOrganizationType($organizationTypeId: ID!) {
  deleteOrganizationType(organizationTypeId: $organizationTypeId) {
    _id
    organizationTypeKey
    organizationTypePath
    parentOrganizationTypeId
    title
    subTitle
    description
    icon
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationTypeId | `ID!` |

Response: `OrganizationType!`

---

#### createOrganization

สร้างข้อมูล Organization

สร้างองค์กรแล้ว ระบบจะสร้าง organizationRole จาก defaultOrganizationRole ทุกตัวให้อัตโนมัติ และสร้างใบอนุมัติองค์กร (ผู้ที่มีสิทธิ์ `autoOrganizationApprove` จะอนุมัติทันที)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createOrganization`

```graphql
mutation CreateOrganization($createInput: CreateOrganizationInput!) {
  createOrganization(createInput: $createInput) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

`createInput`: `CreateOrganizationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| organizationKey | `String` |  | ถ้าไม่ระบุ จะ auto gen |
| parentOrganizationId | `String` |  | _id organization ของแม่ |
| organizationImageKey | `String` |  | fileKey ของรูปภาพ ใช้เป็นรูป profile ของ organization |
| organizationBackgroundImageKey | `String` |  | fileKey ของรูปภาพ ใช้เป็นรูปภาพพื้นหลังของ organization |
| title | `String!` | ใช่ | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| title2 | `String` |  | ชื่อ 2 |
| subTitle2 | `String` |  | ชื่อรอง 2 |
| description2 | `String` |  | รายละเอียด 2 |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isPublic | `Boolean` |  | สถานะการPublic true เมื่อเปิดให้ Public |
| address | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| address2 | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization2 |
| organizationTypeId | `String` |  | _id organizationType ที่ระบุ |
| organizationTagIds | `[String]` |  | array ของ _id organizationTag |

Response: `Organization!`

---

#### updateOrganization

แก้ไขข้อมูล Organization ถ้ามีลูก ห้ามแก้ organizationParent

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganization` (ระดับองค์กร)

```graphql
mutation UpdateOrganization($organizationId: ID!, $updateInput: UpdateOrganizationInput!) {
  updateOrganization(organizationId: $organizationId, updateInput: $updateInput) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

`updateInput`: `UpdateOrganizationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| parentOrganizationId | `String` |  | _id organization ของแม่ |
| organizationImageKey | `String` |  | fileKey ของรูปภาพ ใช้เป็นรูป profile ของ organization |
| organizationBackgroundImageKey | `String` |  | fileKey ของรูปภาพ ใช้เป็นรูปภาพพื้นหลังของ organization |
| title | `String` |  | ชื่อ |
| subTitle | `String` |  | ชื่อรอง ซึ้งอาจจะเป็นชื่อเวอร์ชั่นภาษาอื่น หรือชื่อย่อ |
| description | `String` |  | รายล่ะเอียด |
| title2 | `String` |  | ชื่อ 2 |
| subTitle2 | `String` |  | ชื่อรอง 2 |
| description2 | `String` |  | รายละเอียด 2 |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isPublic | `Boolean` |  | สถานะการPublic true เมื่อเปิดให้ Public |
| address | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization |
| address2 | `OrganizationAddressInput` |  | ข้อมูลที่อยู่ของ organization2 |
| organizationTypeId | `String` |  | _id organizationType ที่ระบุ |
| organizationTagIds | `[String]` |  | array ของ _id organizationTag |

argument อื่น: `organizationId: ID!`

Response: `Organization!`

---

#### deleteOrganization

ลบข้อมูล Organization ถ้ามีลูก ห้ามลบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteOrganization` (ระดับองค์กร)

```graphql
mutation DeleteOrganization($organizationId: ID!) {
  deleteOrganization(organizationId: $organizationId) {
    _id
    organizationKey
    organizationPath
    parentOrganizationId
    organizationImageKey
    organizationBackgroundImageKey
    title
    subTitle
    description
    title2
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| organizationId | `ID!` |

Response: `Organization!`

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
| `refresh-data` | core สั่งให้ส่งข้อมูลที่ถืออยู่ขึ้นไปใหม่ (organization, type, tag, approve, contact, role, running number ฯลฯ และ `sync-permission`) + ล้าง cache |
| `sync-app-certificate` | รับ AppCertificate ของ service ต่อแอป จาก core |
| `sync-app-credential` | รับ AppCredential ของแอป จาก Authentication Service ใช้ตรวจ token / header |
| `sync-service-setting` / `sync-app-service-setting` | รับค่าตั้งค่าเพิ่มเติมแบบ JSON ทั้งระบบ / รายแอป |
| `sync-application` | รับข้อมูลแอป |
| `sync-user-policy` | รับ UserPolicy ของ permission `unit` จาก ACL · ใช้ตรวจสิทธิ์ และใช้รู้ว่าผู้ใช้อยู่องค์กรใด (`getUserOrganizations`) |

---

### add-admin-app-role

เมื่อมีแอปใหม่ core สั่งให้สร้าง appRole `admin` ของแอปนั้น แล้วส่ง `sync-app-role` (พร้อม `isAdmin: true`) ให้ ACL

    topic: add-admin-app-role

| key | Type | คำอธิบาย |
| --- | --- | --- |
| adminAppRole.appKey | string | แอปที่ต้องการสร้าง role admin |
| action | string | `ADD` |

---

### register-custom-running-number

ให้ service อื่นลงทะเบียนรูปแบบเลขรันนิ่งที่ตัวเองใช้

    topic: register-custom-running-number

| key | Type | คำอธิบาย |
| --- | --- | --- |
| customRunningNumber.appKey | string | appKey |
| customRunningNumber.level | string | `APP`, `ORG` |
| customRunningNumber.organizationId / organizationKey | string | องค์กร (เมื่อ level = `ORG`) |
| customRunningNumber.customRunningNumberKey | string | key ของเลขรันนิ่ง |
| customRunningNumber.refKey | string | key อ้างอิงของผู้ใช้งาน |
| customRunningNumber.title / description / order | | ข้อมูลแสดงผล |
| customRunningNumber.serial | string | รอบการนับ `YEAR`, `MONTH`, `DAY`, `INDEX` |
| customRunningNumber.padding | number | จำนวนหลัก |
| customRunningNumber.pattern | string | รูปแบบเลข |
| customRunningNumber.isDefault / isActive | boolean | สถานะ |
| action | string | `ADD` |

---

### generate-running-number

ให้ service อื่นขอเลขรันนิ่งถัดไป · ผลตอบกลับทาง `generated-running-number-result`

    topic: generate-running-number

| key | Type | คำอธิบาย |
| --- | --- | --- |
| customRunNumber.customRunningNumberKey | string | key ของเลขรันนิ่ง |
| customRunNumber.pattern | string | (ไม่บังคับ) pattern ที่ต้องการใช้แทนค่าที่ตั้งไว้ |
| customRunNumber.targetId / targetKey / targetType | string | เอกสารปลายทางที่จะใช้เลขนี้ (ส่งกลับมาในผลลัพธ์) |
| action | string | `ADD` |

---

<br>
<br>

## Kafka Produce Reference

header `appKey` = แอปของข้อมูล · payload ของแต่ละ topic คือข้อมูลของ entity นั้นตาม type ใน API ด้านบน พร้อม `action`

| topic | root key | ส่งเมื่อ |
| --- | --- | --- |
| `sync-organization` | `organization` | สร้าง / แก้ / ลบองค์กร (`id`, `appKey`, `organizationKey`, `organizationPath`, `parentOrganizationId`, `title`, `address`, `organizationTypeId`, `organizationTagIds`, `isApproved`, `isPublic`, `isActive`, ...) |
| `sync-organization-type` | `organizationType` | สร้าง / แก้ / ลบประเภทองค์กร |
| `sync-organization-tag` | `organizationTag` | สร้าง / แก้ / ลบแท็กองค์กร |
| `sync-organization-approve` | `organizationApprove` | สร้างใบอนุมัติ / เปลี่ยนสถานะ (`approveStatus`) |
| `sync-organization-approve-detail` | `organizationApproveDetail` | สร้าง / แก้ / ลบรายละเอียดใบอนุมัติ |
| `sync-organization-contact` | `organizationContact` | สร้าง / แก้ / ลบผู้ติดต่อ |
| `sync-organization-contact-people` | `organizationContactPeople` | สร้าง / แก้ / ลบบุคคลติดต่อ |
| `sync-organization-contact-relation` | `organizationContactRelation` | สร้าง / แก้ / ลบความสัมพันธ์ผู้ติดต่อ |
| `sync-app-role` | `appRole` | สร้าง / แก้ / ลบ appRole |
| `sync-organization-role` | `organizationRole` | สร้าง / แก้ / ลบ organizationRole (รวมที่สร้างอัตโนมัติจากแม่แบบตอนสร้างองค์กร) |
| `sync-default-organization-role` | `defaultOrganizationRole` | สร้าง / แก้ / ลบ defaultOrganizationRole |
| `sync-custom-running-number` | `customRunningNumber` | สร้าง / แก้ / ลบเลขรันนิ่ง |
| `generated-running-number-result` | `generatedRunningNumber` | ตอบผลของ `generate-running-number`: `customRunningNumberKey`, `targetId`, `targetKey`, `targetType`, `generatedCode` |
| `sync-permission` | `permission` | ส่ง permission ของ `unit` ให้ ACL (ตอนได้ `refresh-data`) |

---

> อัปเดตจากโค้ด gumon-unit-service@8cd0bc9 · 2026-10-05
