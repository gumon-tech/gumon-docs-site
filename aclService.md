# ACL Service

Service สำหรับจัดการสิทธิ์ (Access Control) ของทั้งระบบ

    serviceKey: access-control

หน้าที่หลัก

- เป็นทะเบียน **Permission** ของทุก service (แต่ละ service ส่ง permission ของตัวเองมาทาง topic `sync-permission`)
- ผูก Permission และ **Custom Menu** เข้ากับ role 3 ระดับ: `appRole` (ทั้งแอป), `organizationRole` (ต่อองค์กร) และ `defaultOrganizationRole` (แม่แบบ role ที่ทุกองค์กรใหม่จะได้)
- ผูกผู้ใช้เข้ากับ role (`addAppRoleToUser`, `addOrganizationRoleToUser`) และจัดการ **Invite Code** (รหัสเชิญที่ให้ role / องค์กรเริ่มต้นอัตโนมัติตอนสมัคร)
- คอมไพล์สิทธิ์ของผู้ใช้เป็น **UserPolicy** แล้วส่งให้ service เจ้าของ permission ทาง topic `sync-user-policy`

ตัว role (สร้าง / แก้ / ลบ appRole, organizationRole, defaultOrganizationRole) นิยามอยู่ที่ [Unit Service](unitService.md) แล้ว ACL รับสำเนามาทาง Kafka เพื่อผูก permission / เมนู / ผู้ใช้

<br>

- [ลำดับการทำงานของสิทธิ์](#permission-flow)
- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)

---

<a id="permission-flow"></a>

## ลำดับการทำงานของสิทธิ์

**1. service ประกาศ permission ของตัวเองให้ ACL**

    topic: sync-permission

| key | Type | คำอธิบาย |
| --- | --- | --- |
| permission.serviceKey | string | serviceKey ของ service เจ้าของ permission |
| permission.permissionKey | string | key ของ permission (ไม่ซ้ำภายใน service) |
| permission.title / description | string | ชื่อและคำอธิบาย |
| permission.isSystem | boolean | permission ระดับระบบ |
| permission.isGenerateApplication | boolean | สร้าง UserPolicy ระดับแอป |
| permission.isGenerateOrganization | boolean | สร้าง UserPolicy ระดับองค์กร |
| permission.isActive | boolean | `false` = ปิด permission นี้ (ลบการผูกและ UserPolicy ที่เกี่ยวข้อง) |
| action | string | `ADD`, `REMOVE` |

permission เป็นของระบบ ไม่ผูกกับแอป (อ้างอิงด้วยคู่ `serviceKey` + `permissionKey`) · header `serviceKey` ของข้อความนี้ = `access-control`

**2. ผูก permission / เมนู / ผู้ใช้ เข้ากับ role**

ผู้ดูแลผูก permission และเมนูเข้ากับ role และผูกผู้ใช้เข้ากับ role ผ่าน API ด้านล่าง (หรือผ่าน invite code ตอนสมัคร / topic `set-user-role`)

**3. ACL คำนวณ UserPolicy แล้วส่งให้ service เจ้าของ permission**

ถ้า organizationRole เปิด `isChildOrganizationAccess` สิทธิ์จะแตกลงไปถึงองค์กรลูกด้วย · ส่งทาง `sync-user-policy` โดย header `serviceKey` = service ปลายทาง

**4. service ปลายทางตรวจสิทธิ์เองจากข้อมูลที่ถืออยู่**

ตอนรับ request service สร้าง key แล้วค้นในฐานข้อมูล / cache ของตัวเอง

    <serviceKey>::<permissionKey>:<appKey>::<authId>                    (ระดับแอป)
    <serviceKey>::<permissionKey>:<appKey>:<organizationId>:<authId>    (ระดับองค์กร)

ถ้าข้อมูล UserPolicy ของแอปต้องคำนวณใหม่ทั้งหมด ใช้ mutation `reCalculateUserPolicy`

ผู้ที่อยู่ใน appRole ที่เป็น admin ของแอป (`isAdmin`) จะได้ทุก permission และทุกเมนูของแอปโดยอัตโนมัติ

---

<br>
<br>

## API Reference

การยืนยันตัวตนและ header ดูที่ [Authentication Service](authenticationService.md#app-credential) · "สิทธิ์" คือ permissionKey ของ service `access-control`

---

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

---

#### getAppRolesCustomMenus

ดึงข้อมูล getAppRolesCustomMenus ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRolesCustomMenus`

```graphql
query GetAppRolesCustomMenus($input: GetAppRolesCustomMenuInput) {
  getAppRolesCustomMenus(input: $input) {
    appRolesCustomMenus { _id appKey customMenuId customMenuKey customMenuPath appRoleId }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetAppRolesCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAppRolesCustomMenuFilterInput` |  |  |
| search | `GetAppRolesCustomMenuSearchInput` |  |  |
| sort | `GetAppRolesCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `AppRolesCustomMenuPagination`

---

#### getAppRolesCustomMenuById

ดึงข้อมูล getAppRolesCustomMenu ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRolesCustomMenuById`

```graphql
query GetAppRolesCustomMenuById($appRolesCustomMenuId: String!) {
  getAppRolesCustomMenuById(appRolesCustomMenuId: $appRolesCustomMenuId) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    priority
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRolesCustomMenuId | `String!` |

Response: `AppRolesCustomMenu`

---

#### getAppRolesPermissions

ดึงข้อมูล getAppRolesPermissions ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRolesPermissions`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetAppRolesPermissions($input: GetAppRolePermissionInput) {
  getAppRolesPermissions(input: $input) {
    appRolePermissions { _id appKey permissionId permissionKey appRoleId appRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetAppRolePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAppRolePermissionFilterInput` |  |  |
| search | `GetAppRolePermissionSearchInput` |  |  |
| sort | `GetAppRolePermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `AppRolePermissionPagination!`

---

#### getAppRolesPermissionById

ดึงข้อมูล getAppRolesPermissions ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppRolesPermissionByiD`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetAppRolesPermissionById($appRolesPermissionId: ID!) {
  getAppRolesPermissionById(appRolesPermissionId: $appRolesPermissionId) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRolesPermissionId | `ID!` |

Response: `AppRolePermission!`

---

#### getCustomMenus

ดึงข้อมูล customMenu ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomMenus`

```graphql
query GetCustomMenus($getInput: GetCustomMenuInput) {
  getCustomMenus(getInput: $getInput) {
    customMenus { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetCustomMenuFilterInput` |  |  |
| search | `GetCustomMenuSearchInput` |  |  |
| sort | `GetCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `CustomMenuPagination`

---

#### getCustomMenuById

ดึงข้อมูล customMenu ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomMenuById`

```graphql
query GetCustomMenuById($customMenuId: String!) {
  getCustomMenuById(customMenuId: $customMenuId) {
    _id
    serviceKey
    customMenuKey
    parentCustomMenuId
    parentCustomMenuKey
    customMenuPath
    level
    title
    multilingualTitle
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customMenuId | `String!` |

Response: `CustomMenu`

---

#### getCustomMenuByKey

ดึงข้อมูล customMenu ตาม customMenuKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getCustomMenuByKey`

```graphql
query GetCustomMenuByKey($customMenuKey: String!) {
  getCustomMenuByKey(customMenuKey: $customMenuKey) {
    _id
    serviceKey
    customMenuKey
    parentCustomMenuId
    parentCustomMenuKey
    customMenuPath
    level
    title
    multilingualTitle
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customMenuKey | `String!` |

Response: `CustomMenu`

---

#### getDefaultOrganizationRoleCustomMenus

ดึงข้อมูล DefaultOrganizationRoleCustomMenu ทั้งหมด สามารถระบุ appKey หรือ filter อื่น ๆ ได้

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getDefaultOrganizationRoleCustomMenus`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetDefaultOrganizationRoleCustomMenus($input: GetDefaultOrganizationRoleCustomMenuInput) {
  getDefaultOrganizationRoleCustomMenus(input: $input) {
    defaultOrganizationRoleCustomMenus { _id appKey customMenuId customMenuKey defaultOrganizationRoleId defaultOrganizationRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetDefaultOrganizationRoleCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetDefaultOrganizationRoleCustomMenuFilterInput` |  |  |
| search | `GetDefaultOrganizationRoleCustomMenuSearchInput` |  |  |
| sort | `GetDefaultOrganizationRoleCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `DefaultOrganizationRoleCustomMenuPagination!`

---

#### getDefaultOrganizationRoleCustomMenuById

ดึงข้อมูล DefaultOrganizationRoleCustomMenu ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getDefaultOrganizationRoleCustomMenuById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetDefaultOrganizationRoleCustomMenuById($defaultOrganizationRoleCustomMenuId: ID!) {
  getDefaultOrganizationRoleCustomMenuById(defaultOrganizationRoleCustomMenuId: $defaultOrganizationRoleCustomMenuId) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    defaultOrganizationRoleId
    defaultOrganizationRoleKey
    defaultOrganizationRole { _id appKey defaultOrganizationRoleKey title subTitle description }
    systemNote
    priority
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| defaultOrganizationRoleCustomMenuId | `ID!` |

Response: `DefaultOrganizationRoleCustomMenu!`

---

#### getDefaultOrganizationRolePermissions

ดึงข้อมูล getDefaultOrganizationRolePermissions ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getDefaultOrganizationRolePermissions`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetDefaultOrganizationRolePermissions($input: GetDefaultOrgRolePermissionInput) {
  getDefaultOrganizationRolePermissions(input: $input) {
    defaultOrgRolePermissions { _id appKey permissionId permissionKey defaultOrganizationRoleId defaultOrganizationRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetDefaultOrgRolePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetDefaultOrgRolePermissionFilterInput` |  |  |
| search | `GetDefaultOrgRolePermissionSearchInput` |  |  |
| sort | `GetDefaultOrgRolePermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `DefaultOrgRolePermissionPagination!`

---

#### getDefaultOrganizationRolePermissionById

ดึงข้อมูล getDefaultOrganizationRolePermissions ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getDefaultOrganizationRolePermissionById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetDefaultOrganizationRolePermissionById($defaultOrgRolePermissionId: ID!) {
  getDefaultOrganizationRolePermissionById(defaultOrgRolePermissionId: $defaultOrgRolePermissionId) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    defaultOrganizationRoleId
    defaultOrganizationRoleKey
    defaultOrganizationRole { _id appKey defaultOrganizationRoleKey title subTitle description }
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| defaultOrgRolePermissionId | `ID!` |

Response: `DefaultOrgRolePermission!`

---

#### getInviteCodeAppRoles

ดึงข้อมูล Invite Code App Role ทั้งหมดของ app

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeAppRoles`

```graphql
query GetInviteCodeAppRoles($getInput: GetInviteCodeAppRoleInput) {
  getInviteCodeAppRoles(getInput: $getInput) {
    inviteCodeAppRoles { _id inviteCodeAppRoleKey inviteCodeId inviteCodeKey appRoleId appRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeAppRoleFilterInput` |  |  |
| search | `GetInviteCodeAppRoleSearchInput` |  |  |
| sort | `GetInviteCodeAppRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `InviteCodeAppRolePagination`

---

#### getInviteCodeAppRolesByInviteCodeKey

ดึงข้อมูล Invite Code App Role โดย invite code key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeAppRolesByInviteCodeKey`

```graphql
query GetInviteCodeAppRolesByInviteCodeKey($inviteCodeKey: String!, $getInput: GetInviteCodeAppRoleByInviteCodeKeyInput) {
  getInviteCodeAppRolesByInviteCodeKey(inviteCodeKey: $inviteCodeKey, getInput: $getInput) {
    inviteCodeAppRoles { _id inviteCodeAppRoleKey inviteCodeId inviteCodeKey appRoleId appRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeAppRoleByInviteCodeKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeAppRoleByInviteCodeKeyFilterInput` |  |  |
| search | `GetInviteCodeAppRoleByInviteCodeKeySearchInput` |  |  |
| sort | `GetInviteCodeAppRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `inviteCodeKey: String!`

Response: `InviteCodeAppRolePagination`

---

#### getInviteCodeAppRolesByAppRoleKey

ดึงข้อมูล Invite Code App Role โดย app role key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeAppRolesByAppRoleKey`

```graphql
query GetInviteCodeAppRolesByAppRoleKey($appRoleKey: String!, $getInput: GetInviteCodeAppRoleByAppRoleKeyInput) {
  getInviteCodeAppRolesByAppRoleKey(appRoleKey: $appRoleKey, getInput: $getInput) {
    inviteCodeAppRoles { _id inviteCodeAppRoleKey inviteCodeId inviteCodeKey appRoleId appRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeAppRoleByAppRoleKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeAppRoleByAppRoleKeyFilterInput` |  |  |
| search | `GetInviteCodeAppRoleByAppRoleKeySearchInput` |  |  |
| sort | `GetInviteCodeAppRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appRoleKey: String!`

Response: `InviteCodeAppRolePagination`

---

#### getInviteCodeAppRoleById

ดึงข้อมูล Invite Code App Role โดย _id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeAppRoleById`

```graphql
query GetInviteCodeAppRoleById($id: String!) {
  getInviteCodeAppRoleById(id: $id) {
    _id
    inviteCodeAppRoleKey
    inviteCodeId
    inviteCodeKey
    inviteCode { _id inviteCodeKey title subTitle description systemNote }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| id | `String!` |

Response: `InviteCodeAppRole`

---

#### getInviteCodeAppRoleByKey

ดึงข้อมูล Invite Code App Role โดย key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeAppRoleByKey`

```graphql
query GetInviteCodeAppRoleByKey($inviteCodeAppRoleKey: String!) {
  getInviteCodeAppRoleByKey(inviteCodeAppRoleKey: $inviteCodeAppRoleKey) {
    _id
    inviteCodeAppRoleKey
    inviteCodeId
    inviteCodeKey
    inviteCode { _id inviteCodeKey title subTitle description systemNote }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| inviteCodeAppRoleKey | `String!` |

Response: `InviteCodeAppRole`

---

#### getInviteCodeOrgRoles

ดึงข้อมูล Invite Code Org Role ทั้งหมดของ app

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeOrgRoles`

```graphql
query GetInviteCodeOrgRoles($getInput: GetInviteCodeOrgRoleInput) {
  getInviteCodeOrgRoles(getInput: $getInput) {
    inviteCodeOrgRoles { _id inviteCodeOrgRoleKey inviteCodeId inviteCodeKey orgRoleId orgRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeOrgRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeOrgRoleFilterInput` |  |  |
| search | `GetInviteCodeOrgRoleSearchInput` |  |  |
| sort | `GetInviteCodeOrgRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `InviteCodeOrgRolePagination`

---

#### getInviteCodeOrgRolesByInviteCodeKey

ดึงข้อมูล Invite Code Org Role โดย invite code key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeOrgRolesByInviteCodeKey`

```graphql
query GetInviteCodeOrgRolesByInviteCodeKey($inviteCodeKey: String!, $getInput: GetInviteCodeOrgRoleByInviteCodeKeyInput) {
  getInviteCodeOrgRolesByInviteCodeKey(inviteCodeKey: $inviteCodeKey, getInput: $getInput) {
    inviteCodeOrgRoles { _id inviteCodeOrgRoleKey inviteCodeId inviteCodeKey orgRoleId orgRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeOrgRoleByInviteCodeKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeOrgRoleByInviteCodeKeyFilterInput` |  |  |
| search | `GetInviteCodeOrgRoleByInviteCodeKeySearchInput` |  |  |
| sort | `GetInviteCodeOrgRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `inviteCodeKey: String!`

Response: `InviteCodeOrgRolePagination`

---

#### getInviteCodeOrgRolesByOrgRoleKey

ดึงข้อมูล Invite Code Org Role โดย org role key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeOrgRolesByOrgRoleKey`

```graphql
query GetInviteCodeOrgRolesByOrgRoleKey($orgRoleKey: String!, $getInput: GetInviteCodeOrgRoleByOrgRoleKeyInput) {
  getInviteCodeOrgRolesByOrgRoleKey(orgRoleKey: $orgRoleKey, getInput: $getInput) {
    inviteCodeOrgRoles { _id inviteCodeOrgRoleKey inviteCodeId inviteCodeKey orgRoleId orgRoleKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeOrgRoleByOrgRoleKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeOrgRoleBOrgRoleKeyFilterInput` |  |  |
| search | `GetInviteCodeOrgRoleBOrgRoleKeySearchInput` |  |  |
| sort | `GetInviteCodeOrgRoleSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `orgRoleKey: String!`

Response: `InviteCodeOrgRolePagination`

---

#### getInviteCodeOrgRoleById

ดึงข้อมูล Invite Code Org Role โดย _id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeOrgRoleById`

```graphql
query GetInviteCodeOrgRoleById($id: String!) {
  getInviteCodeOrgRoleById(id: $id) {
    _id
    inviteCodeOrgRoleKey
    inviteCodeId
    inviteCodeKey
    inviteCode { _id inviteCodeKey title subTitle description systemNote }
    orgRoleId
    orgRoleKey
    orgRole { _id organizationKey organizationRoleKey title subTitle description }
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| id | `String!` |

Response: `InviteCodeOrgRole`

---

#### getInviteCodeOrgRoleByKey

ดึงข้อมูล Invite Code Org Role โดย key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeOrgRoleByKey`

```graphql
query GetInviteCodeOrgRoleByKey($inviteCodeOrgRoleKey: String!) {
  getInviteCodeOrgRoleByKey(inviteCodeOrgRoleKey: $inviteCodeOrgRoleKey) {
    _id
    inviteCodeOrgRoleKey
    inviteCodeId
    inviteCodeKey
    inviteCode { _id inviteCodeKey title subTitle description systemNote }
    orgRoleId
    orgRoleKey
    orgRole { _id organizationKey organizationRoleKey title subTitle description }
    createdBy
    updatedBy
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| inviteCodeOrgRoleKey | `String!` |

Response: `InviteCodeOrgRole`

---

#### getInviteCodeUsages

ดึงข้อมูล Invite Code Usage ทั้งหมดของ app

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeUsages`

```graphql
query GetInviteCodeUsages($getInput: GetInviteCodeUsageInput) {
  getInviteCodeUsages(getInput: $getInput) {
    inviteCodeUsages { _id authId inviteCodeId inviteCodeKey email countryCode }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeUsageInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeUsageFilterInput` |  |  |
| search | `GetInviteCodeUsageSearchInput` |  |  |
| sort | `GetInviteCodeUsageSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `InviteCodeUsagePagination`

---

#### getInviteCodeUsagesByInviteCodeKey

ดึงข้อมูล Invite Code Usage โดยใช้ invite code key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeUsagesByInviteCodeKey`

```graphql
query GetInviteCodeUsagesByInviteCodeKey($inviteCodeKey: String!, $getInput: GetInviteCodeUsageByInviteCodeKeyInput) {
  getInviteCodeUsagesByInviteCodeKey(inviteCodeKey: $inviteCodeKey, getInput: $getInput) {
    inviteCodeUsages { _id authId inviteCodeId inviteCodeKey email countryCode }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeUsageByInviteCodeKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeUsageByInviteCodeKeyFilterInput` |  |  |
| search | `GetInviteCodeUsageByInviteCodeKeySearchInput` |  |  |
| sort | `GetInviteCodeUsageSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `inviteCodeKey: String!`

Response: `InviteCodeUsagePagination`

---

#### getInviteCodeUsageById

ดึงข้อมูล Invite Code Usage โดยใช้ id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeUsageById`

```graphql
query GetInviteCodeUsageById($id: String!) {
  getInviteCodeUsageById(id: $id) {
    _id
    authId
    inviteCodeId
    inviteCodeKey
    inviteCode { _id inviteCodeKey title subTitle description systemNote }
    email
    countryCode
    phoneNumber
    username
    redirectUrl
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| id | `String!` |

Response: `InviteCodeUsage`

---

#### getInviteCodeUsageByAuthId

ดึงข้อมูล Invite Code Usage โดยใช้ authId

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeUsageByAuthId`

```graphql
query GetInviteCodeUsageByAuthId($authId: String!) {
  getInviteCodeUsageByAuthId(authId: $authId) {
    _id
    authId
    inviteCodeId
    inviteCodeKey
    inviteCode { _id inviteCodeKey title subTitle description systemNote }
    email
    countryCode
    phoneNumber
    username
    redirectUrl
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| authId | `String!` |

Response: `InviteCodeUsage`

---

#### getInviteCodes

ดึงข้อมูล Invite Code ทั้งหมดของ app

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodes`

```graphql
query GetInviteCodes($getInput: GetInviteCodeInput) {
  getInviteCodes(getInput: $getInput) {
    inviteCodes { _id inviteCodeKey title subTitle description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetInviteCodeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetInviteCodeFilterInput` |  |  |
| search | `GetInviteCodeSearchInput` |  |  |
| sort | `GetInviteCodeSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `InviteCodePagination`

---

#### getPublicInviteCodes

ดึงข้อมูล Invite Code ของ app ที่เป็น public

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPublicInviteCodes($getInput: GetPublicInviteCodeInput) {
  getPublicInviteCodes(getInput: $getInput) {
    inviteCodes { _id inviteCodeKey title subTitle description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetPublicInviteCodeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPublicInviteCodeFilterInput` |  |  |
| search | `GetInviteCodeSearchInput` |  |  |
| sort | `GetInviteCodeSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `InviteCodePagination`

---

#### getInviteCodeById

ดึงข้อมูล Invite Code โดยใช้ id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeById`

```graphql
query GetInviteCodeById($id: String!) {
  getInviteCodeById(id: $id) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| id | `String!` |

Response: `InviteCode`

---

#### getInviteCodeByKey

ดึงข้อมูล Invite Code โดยใช้ key

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getInviteCodeByKey`

```graphql
query GetInviteCodeByKey($inviteCodeKey: String!) {
  getInviteCodeByKey(inviteCodeKey: $inviteCodeKey) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| inviteCodeKey | `String!` |

Response: `InviteCode`

---

#### getPublicInviteCodeById

ดึงข้อมูล Invite Code โดยใช้ id

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPublicInviteCodeById($id: String!) {
  getPublicInviteCodeById(id: $id) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| id | `String!` |

Response: `InviteCode`

---

#### getPublicInviteCodeByKey

ดึงข้อมูล Invite Code โดยใช้ key

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPublicInviteCodeByKey($inviteCodeKey: String!) {
  getPublicInviteCodeByKey(inviteCodeKey: $inviteCodeKey) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| inviteCodeKey | `String!` |

Response: `InviteCode`

---

#### getOrganizationRolesCustomMenus

ดึงข้อมูล getOrgRolesCustomMenus ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRolesCustomMenus`

```graphql
query GetOrganizationRolesCustomMenus($input: GetOrgRoleCustomMenuInput) {
  getOrganizationRolesCustomMenus(input: $input) {
    organizationRolesCustomMenus { _id appKey customMenuId customMenuKey customMenuPath organizationId }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrgRoleCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrgRoleCustomMenuFilterInput` |  |  |
| search | `GetOrgRoleCustomMenuSearchInput` |  |  |
| sort | `GetOrgRoleCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrgRoleCustomMenuPagination`

---

#### getOrganizationRolesCustomMenuById

ดึงข้อมูล getOrgRolesCustomMenu ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRolesCustomMenuById`

```graphql
query GetOrganizationRolesCustomMenuById($orgRolesCustomMenuId: String) {
  getOrganizationRolesCustomMenuById(orgRolesCustomMenuId: $orgRolesCustomMenuId) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    organizationId
    organizationKey
    organizationPath
    organizationRoleId
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| orgRolesCustomMenuId | `String` |

Response: `OrgRolesCustomMenu`

---

#### getOrganizationRolesPermissions

ดึงข้อมูล getOrgRolesPermissions ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRolesPermissions`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetOrganizationRolesPermissions($input: GetOrgRolePermissionInput) {
  getOrganizationRolesPermissions(input: $input) {
    orgRolePermissions { _id appKey permissionId permissionKey organizationId organizationKey }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetOrgRolePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetOrgRolePermissionFilterInput` |  |  |
| search | `GetOrgRolePermissionSearchInput` |  |  |
| sort | `GetOrgRolePermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `OrgRolePermissionPagination!`

---

#### getOrganizationRolesPermissionById

ดึงข้อมูล getOrgRolesPermissions ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getOrganizationRolesPermissionById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetOrganizationRolesPermissionById($orgRolesPermissionId: ID!) {
  getOrganizationRolesPermissionById(orgRolesPermissionId: $orgRolesPermissionId) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    organizationId
    organizationKey
    organizationPath
    organizationRoleId
    organizationRoleKey
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| orgRolesPermissionId | `ID!` |

Response: `OrgRolePermission!`

---

#### getPermissions

ดึงข้อมูล permissions ทั้งหมดที่มีในระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissions`

```graphql
query GetPermissions($input: GetPermissionInput) {
  getPermissions(input: $input) {
    permissions { _id serviceKey permissionKey title description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetPermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPermissionFilterInput` |  |  |
| search | `GetPermissionSearchInput` |  |  |
| sort | `GetPermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `PermissionPagination!`

---

#### getPermissionById

ดึงข้อมูล permissions ตาม ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissionById`

```graphql
query GetPermissionById($permissionId: ID!) {
  getPermissionById(permissionId: $permissionId) {
    _id
    serviceKey
    permissionKey
    title
    description
    systemNote
    isSystem
    isGenerateApplication
    isGenerateOrganization
    appUserPolicyKeyPattern
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| permissionId | `ID!` |

Response: `Permission!`

---

#### getPermissionByKey

ดึงข้อมูล permissions ตาม permissionKey

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissionByKey`

```graphql
query GetPermissionByKey($permissionKey: String!) {
  getPermissionByKey(permissionKey: $permissionKey) {
    _id
    serviceKey
    permissionKey
    title
    description
    systemNote
    isSystem
    isGenerateApplication
    isGenerateOrganization
    appUserPolicyKeyPattern
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| permissionKey | `String!` |

Response: `Permission!`

---

#### getPermissionsByAppKey

ดึงข้อมูล permissions ที่อยู่ใน appKey ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissions`

```graphql
query GetPermissionsByAppKey($appKey: String!, $input: GetPermissionInput) {
  getPermissionsByAppKey(appKey: $appKey, input: $input) {
    permissions { _id serviceKey permissionKey title description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetPermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPermissionFilterInput` |  |  |
| search | `GetPermissionSearchInput` |  |  |
| sort | `GetPermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String!`

Response: `PermissionPagination!`

---

#### getPermissionsUnassignedToAppRole

ดึงข้อมูล permissions ไม่อยู่ใน appRole ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissions`

```graphql
query GetPermissionsUnassignedToAppRole($appRoleId: ID!, $input: GetPermissionInput) {
  getPermissionsUnassignedToAppRole(appRoleId: $appRoleId, input: $input) {
    permissions { _id serviceKey permissionKey title description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetPermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPermissionFilterInput` |  |  |
| search | `GetPermissionSearchInput` |  |  |
| sort | `GetPermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appRoleId: ID!`

Response: `PermissionPagination!`

---

#### getPermissionsUnassignedToOrganizationRole

ดึงข้อมูล permissions ไม่อยู่ใน organizationRole ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissions`

```graphql
query GetPermissionsUnassignedToOrganizationRole($organizationRoleId: ID!, $input: GetPermissionInput) {
  getPermissionsUnassignedToOrganizationRole(organizationRoleId: $organizationRoleId, input: $input) {
    permissions { _id serviceKey permissionKey title description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetPermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPermissionFilterInput` |  |  |
| search | `GetPermissionSearchInput` |  |  |
| sort | `GetPermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `organizationRoleId: ID!`

Response: `PermissionPagination!`

---

#### getPermissionsUnassignedToDefaultOrgRole

ดึงข้อมูล permissions ไม่อยู่ใน defaultOrgRole ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getPermissions`

```graphql
query GetPermissionsUnassignedToDefaultOrgRole($defaultOrgRoleId: ID!, $input: GetPermissionInput) {
  getPermissionsUnassignedToDefaultOrgRole(defaultOrgRoleId: $defaultOrgRoleId, input: $input) {
    permissions { _id serviceKey permissionKey title description systemNote }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetPermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPermissionFilterInput` |  |  |
| search | `GetPermissionSearchInput` |  |  |
| sort | `GetPermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `defaultOrgRoleId: ID!`

Response: `PermissionPagination!`

---

#### getProfileWithAppRoles

ดึงข้อมูล AppRoles ที่มีความเชื่อมโยงกับ user ที่เข้าใช้งาน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfileWithAppRoles`

```graphql
query GetProfileWithAppRoles {
  getProfileWithAppRoles {
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

Response: `ProfileWithAppRoles`

---

#### getProfileWithOrgRoles

ดึงข้อมูล OrgRoles ที่มีความเชื่อมโยงกับ user ที่เข้าใช้งาน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getProfileWithOrgRoles`

```graphql
query GetProfileWithOrgRoles($organizationKey: String!) {
  getProfileWithOrgRoles(organizationKey: $organizationKey) {
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
| organizationKey | `String!` |

Response: `ProfileWithOrgRoles!`

---

#### getUserAppRoles

Get UserAppRoles ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserAppRoles`

```graphql
query GetUserAppRoles($input: GetUserAppRolesInput) {
  getUserAppRoles(input: $input) {
    userAppRoles { _id authId appRoleId appRoleKey isActive createdAt }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserAppRolesInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserAppRolesFilterInput` |  |  |
| search | `GetUserAppRolesSearchInput` |  |  |
| sort | `GetUserAppRolesSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `UserAppRolesPagination!`

---

#### getUserAppRoleById

Get UserAppRole ในการใช้งานระบบ โดยใช้ UserAppRoleId ในการระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserAppRoleById`

```graphql
query GetUserAppRoleById($userAppRoleId: ID!) {
  getUserAppRoleById(userAppRoleId: $userAppRoleId) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    isActive
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| userAppRoleId | `ID!` |

Response: `UserAppRole!`

---

#### getMyAppRoles

Get MyAppRoles ในการใช้งานระบบ โดยดึงข้อมูลมาจาก token ผู้ใช้งาน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyAppRoles {
  getMyAppRoles {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    isActive
    createdAt
    updatedAt
  }
}
```

Response: `[UserAppRole]!`

---

#### getMyAppRoleByID

Get MyAppRoles ในการใช้งานระบบ โดยดึงข้อมูลมาจาก token ผู้ใช้งาน และ ใช้ UserAppRoleId ในการระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyAppRoleByID($userAppRoleId: ID!) {
  getMyAppRoleByID(userAppRoleId: $userAppRoleId) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    isActive
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| userAppRoleId | `ID!` |

Response: `UserAppRole!`

---

#### getUserAppRolesByAppKey

Get UserAppRoles ของ User ตาม AppKey ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetUserAppRolesByAppKey($appKey: String!, $input: GetUserAppRolesInput) {
  getUserAppRolesByAppKey(appKey: $appKey, input: $input) {
    userAppRoles { _id authId appRoleId appRoleKey isActive createdAt }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserAppRolesInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserAppRolesFilterInput` |  |  |
| search | `GetUserAppRolesSearchInput` |  |  |
| sort | `GetUserAppRolesSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String!`

Response: `UserAppRolesPagination!`

---

#### getUserCustomMenus

ดึงข้อมูล getUserCustomMenus ตามที่ค้นหา และสามารถ ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserCustomMenus`

```graphql
query GetUserCustomMenus($input: GetUserCustomMenuInput) {
  getUserCustomMenus(input: $input) {
    userCustomMenus { _id appKey authId customMenuId customMenuKey customMenuPath }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserCustomMenuFilterInput` |  |  |
| search | `GetUserCustomMenuSearchInput` |  |  |
| sort | `GetUserCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `UserCustomMenuPagination`

---

#### getUserCustomMenuById

ดึงข้อมูล getUserCustomMenuById ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserCustomMenuById`

```graphql
query GetUserCustomMenuById($userCustomMenuId: ID!) {
  getUserCustomMenuById(userCustomMenuId: $userCustomMenuId) {
    _id
    appKey
    authId
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    organizationId
    organizationKey
    organizationPath
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| userCustomMenuId | `ID!` |

Response: `UserCustomMenu`

---

#### getUserCustomMenusByAuthId

ดึงข้อมูล getUserCustomMenusByAuthId ตาม authId ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserCustomMenusByAuthId`

```graphql
query GetUserCustomMenusByAuthId($authId: String!, $input: GetUserCustomMenuInputByAuthId) {
  getUserCustomMenusByAuthId(authId: $authId, input: $input) {
    customMenus { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserCustomMenuInputByAuthId`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserCustomMenuByAuthIdFilterInput` |  |  |
| search | `GetUserCustomMenuByAuthIdSearchInput` |  |  |
| sort | `GetUserCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `authId: String!`

Response: `CustomMenuPagination`

---

#### getUserCustomMenusByLevel

ดึงข้อมูล getUserCustomMenesByLevel ตาม level ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserCustomMenesByLevel`

```graphql
query GetUserCustomMenusByLevel($level: EnumLevel!, $input: GetUserCustomMenuInputByLevel) {
  getUserCustomMenusByLevel(level: $level, input: $input) {
    userCustomMenus { _id appKey authId customMenuId customMenuKey customMenuPath }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserCustomMenuInputByLevel`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserCustomMenuByLevelFilterInput` |  |  |
| search | `GetUserCustomMenuByLevelSearchInput` |  |  |
| sort | `GetUserCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `level: EnumLevel!`

Response: `UserCustomMenuPagination`

---

#### getMyCustomMenus

ดึงข้อมูล getMyCustomMenus ของ user ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyCustomMenus($input: GetUserCustomMenuInputByAuthId) {
  getMyCustomMenus(input: $input) {
    customMenus { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserCustomMenuInputByAuthId`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserCustomMenuByAuthIdFilterInput` |  |  |
| search | `GetUserCustomMenuByAuthIdSearchInput` |  |  |
| sort | `GetUserCustomMenuSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `CustomMenuPagination`

---

#### getUserOrgRoles

Get UserOrgRoles ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserOrgRoles`

```graphql
query GetUserOrgRoles($input: GetUserOrgRolesInput) {
  getUserOrgRoles(input: $input) {
    userOrgRoles { _id authId organizationRoleKey organizationKey organizationPath isActive }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserOrgRolesInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserOrgRolesFilterInput` |  |  |
| search | `GetUserOrgRolesSearchInput` |  |  |
| sort | `GetUserOrgRolesSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `UserOrgRolesPagination!`

---

#### getUserOrgRoleById

Get UserOrgRoles ในการใช้งานระบบ โดยใช้ UserOrgRoleId ในการระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserOrgRoleById`

```graphql
query GetUserOrgRoleById($userOrgRoleId: ID!) {
  getUserOrgRoleById(userOrgRoleId: $userOrgRoleId) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    organizationRoleKey
    organizationRole { _id organizationKey organizationRoleKey title subTitle description }
    organizationKey
    organizationPath
    isActive
    createdAt
    updatedAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| userOrgRoleId | `ID!` |

Response: `UserOrgRole!`

---

#### getMyOrgRoles

Get MyOrgRoles ในการใช้งานระบบ โดยดึงข้อมูลมาจาก token ผู้ใช้งาน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyOrgRoles {
  getMyOrgRoles {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    organizationRoleKey
    organizationRole { _id organizationKey organizationRoleKey title subTitle description }
    organizationKey
    organizationPath
    isActive
    createdAt
    updatedAt
    # ...
  }
}
```

Response: `[UserOrgRole]!`

---

#### getMyOrgRoleByID

Get MyOrgRole ในการใช้งานระบบ โดยดึงข้อมูลมาจาก token ผู้ใช้งาน และ ใช้ UserOrgRoleId ในการระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyOrgRoleByID($userOrgRoleId: ID!) {
  getMyOrgRoleByID(userOrgRoleId: $userOrgRoleId) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    organizationRoleKey
    organizationRole { _id organizationKey organizationRoleKey title subTitle description }
    organizationKey
    organizationPath
    isActive
    createdAt
    updatedAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| userOrgRoleId | `ID!` |

Response: `UserOrgRole!`

---

#### getUserPermissions

ดึงข้อมูล getUserPermissions ตามที่ค้นหา และสามารถ ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserPermissions`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetUserPermissions($input: GetUserPermissionInput) {
  getUserPermissions(input: $input) {
    userPermissions { _id appKey authId permissionId permissionKey isGenerateApplication }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserPermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserPermissionFilterInput` |  |  |
| search | `GetUserPermissionSearchInput` |  |  |
| sort | `GetUserPermissionSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `UserPermissionPagination!`

---

#### getUserPermissionById

ดึงข้อมูล getUserPermissionById ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserPermissionById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetUserPermissionById($userPermissionId: ID!) {
  getUserPermissionById(userPermissionId: $userPermissionId) {
    _id
    appKey
    authId
    permissionId
    permissionKey
    isGenerateApplication
    isGenerateOrganization
    userPolicyKeyPattern
    organizationId
    organizationKey
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| userPermissionId | `ID!` |

Response: `UserPermission!`

---

#### getUserPolicys

ดึงข้อมูล getUserPolicys ตามที่ค้นหา และสามารถ ระบุ appKey อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserPolicys`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetUserPolicys($input: GetUserPolicyInput) {
  getUserPolicys(input: $input) {
    userPolicys { _id appKey userPolicyKey authId permissionKey organizationId }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetUserPolicyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetUserPolicyFilterInput` |  |  |
| search | `GetUserPolicySearchInput` |  |  |
| sort | `GetUserPolicySortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `UserPolicyPagination!`

---

#### getUserPolicyById

ดึงข้อมูล getUserPolicyById ตาม ID ถ้าอยากค้นหาข้าม app อื่นได้ แต่ต้องมีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getUserPolicyById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetUserPolicyById($userPolicyId: ID!) {
  getUserPolicyById(userPolicyId: $userPolicyId) {
    _id
    appKey
    userPolicyKey
    authId
    permissionKey
    organizationId
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| userPolicyId | `ID!` |

Response: `UserPolicy!`

---

#### verifyUserPolicy

ใช้สำหรับตรวจสอบ list ของ userPolicyKey ว่าแต่สิทธิ์ที่ส่งมามีสิทธิ์ใช้งานหรือไม่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query VerifyUserPolicy($userPolicyKeys: [String!]!) {
  verifyUserPolicy(userPolicyKeys: $userPolicyKeys) {
    userPolicyKey
    isHavePolicy
  }
}
```

| argument | Type |
| --- | --- |
| userPolicyKeys | `[String!]!` |

Response: `[ResultVerifyUserPolicy]!`

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

---

#### addCustomMenuToAppRole

เพิ่ม CustomMenu ลงใน AppRole ทำข้าม app ได้ โดยเช็กจาก appRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addCustomMenuToAppRole`

```graphql
mutation AddCustomMenuToAppRole($input: AddCustomMenuToAppRoleInput!) {
  addCustomMenuToAppRole(input: $input)
}
```

`input`: `AddCustomMenuToAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| customMenuIds | `[String]` |  | customMenuId ใช้เพื่อบอกว่า เป็น id ของ customMenu ไหน |
| appRoleIds | `[String]` |  | appRoleId ใช้เพื่อบอกว่า เป็น id ของ appRole ไหน |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `Boolean`

---

#### removeCustomMenuFromAppRole

ลบ CustomMenu จาก AppRole ทำข้าม app ได้ โดยเช็กจาก appRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeCustomMenuFromAppRole`

```graphql
mutation RemoveCustomMenuFromAppRole($appRolesCustomMenuId: String!) {
  removeCustomMenuFromAppRole(appRolesCustomMenuId: $appRolesCustomMenuId) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    priority
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRolesCustomMenuId | `String!` |

Response: `AppRolesCustomMenu`

---

#### updateAppRolesCustomMenu

แก้ไขข้อมูล AppRolesCustomMenu ตาม id ทำข้าม app ได้ โดยเช็กจาก appRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateAppRolesCustomMenu`

```graphql
mutation UpdateAppRolesCustomMenu($appRolesCustomMenuId: String!, $input: UpdateAppRolesCustomMenuInput!) {
  updateAppRolesCustomMenu(appRolesCustomMenuId: $appRolesCustomMenuId, input: $input) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    priority
    # ...
  }
}
```

`input`: `UpdateAppRolesCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `appRolesCustomMenuId: String!`

Response: `AppRolesCustomMenu!`

---

#### addPermissionToAppRole

เพิ่ม Permission ลงใน AppRole ทำข้าม app ได้ โดยเช็กจาก appRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addPermissionToAppRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddPermissionToAppRole($input: AddPermissionToAppRoleInput!) {
  addPermissionToAppRole(input: $input)
}
```

`input`: `AddPermissionToAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| permissionId | `String` |  | permissionId ใช้เพื่อบอกว่า เป็น id ของ permission ไหน |
| appRoleId | `String` |  | appRoleId ใช้เพื่อบอกว่า เป็น id ของ appRole ไหน |
| permissionIds | `[String]` |  | permissionIds สำหรับใช้ในการระบุ List Permission ที่ต้องการให้ AppRole ใช้งาน |
| appRoleIds | `[String]` |  | appRoleIds สำหรับใช้ในการระบุ List AppRole ที่ต้องการให้ใช้งาน permission |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `Boolean!`

---

#### removePermissionFromAppRole

ลบ Permission จาก AppRole ทำข้าม app ได้ โดยเช็กจาก appRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removePermissionFromAppRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemovePermissionFromAppRole($appRolesPermissionId: ID!) {
  removePermissionFromAppRole(appRolesPermissionId: $appRolesPermissionId) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appRolesPermissionId | `ID!` |

Response: `AppRolePermission!`

---

#### updateAppRolePermission

แก้ไขข้อมูล AppRolePermission ตาม id ทำข้าม app ได้ โดยเช็กจาก appRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateAppRolePermission`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateAppRolePermission($appRolesPermissionId: ID!, $input: UpdateAppRolePermissionInput!) {
  updateAppRolePermission(appRolesPermissionId: $appRolesPermissionId, input: $input) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    systemNote
    isActive
    # ...
  }
}
```

`input`: `UpdateAppRolePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน เมื่อเป็น false จะลบ userPolicy, userAppRolePermission เมื่อแก้กลับเป็น true gen userPolicy,userAppRolePermission |

argument อื่น: `appRolesPermissionId: ID!`

Response: `AppRolePermission!`

---

#### createCustomMenu

สร้างข้อมูล CustomMenu

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createCustomMenu`

```graphql
mutation CreateCustomMenu($createInput: CreateCustomMenuInput!) {
  createCustomMenu(createInput: $createInput) {
    _id
    serviceKey
    customMenuKey
    parentCustomMenuId
    parentCustomMenuKey
    customMenuPath
    level
    title
    multilingualTitle
    description
    # ...
  }
}
```

`createInput`: `CreateCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| serviceKey | `String` |  | serviceKey ใช้เพื่อบอกว่า เป็น ของ service ไหน ไม่ใส่ default เป็น service acl |
| customMenuKey | `String` |  | key ของ custom menu |
| parentCustomMenuId | `String` |  | id ของ parent menu |
| level | `EnumLevel!` | ใช่ | ใช้เพื่อเอามาแบ่งตามสิทธิ |
| title | `String!` | ใช่ | ชื่อที่แสดงบนเมนู |
| multilingualTitle | `JSON` |  | ชื่อที่แสดงบนเมนู (หลายภาษา) |
| description | `String` |  | คำอธิบายสำหรับผู้ดูแลระบบ |
| icon | `String` |  | ไอคอน (เช่นชื่อ material icon) |
| type | `EnumCustomMenuType!` | ใช่ | type ของเมนู |
| templatePath | `String` |  | Path ที่มี dynamic param ได้ เช่น /org/{orgKey}/edit/{itemId} |
| pathParams | `[String]` |  | รายชื่อ param ใน path |
| queryParams | `[String]` |  | รายชื่อ query string param ที่ต้องแนบ (ถ้าต้องการ) |
| url | `String` |  | สำหรับ external-link หรือ iframe |
| openInNewTab | `Boolean` |  | สำหรับ external-link |
| fileName | `String` |  | สำหรับ external-download |
| priority | `Int` |  | ใช้ตัดสินใจเมื่อ route ตรงกับหลายอัน (ยิ่งน้อยยิ่งสำคัญกว่า) |
| order | `Int` |  | ลำดับแสดงผลบนหน้าจอ (UI เท่านั้น, ไม่มีผลกับ route) |
| showBadgeCount | `Boolean` |  | ใช้แสดง badge count |
| badgeCountKey | `String` |  | Key ใช้ดึงค่าจำนวน badge |
| breadCrumbs | `[CreateBreadCrumbInput]` |  | breadCrumbs รายชื่อ breadcrumb ที่จะแสดงบนเมนูนี้ |
| isGroupMenu | `Boolean` |  | เป็น group menu หรือไม่ |
| … | | | (มีอีก 4 ฟิลด์ ดู schema) |

Response: `CustomMenu`

---

#### updateCustomMenu

แก้ไขข้อมูล CustomMenu

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateCustomMenu`

```graphql
mutation UpdateCustomMenu($customMenuId: String!, $updateInput: UpdateCustomMenuInput!) {
  updateCustomMenu(customMenuId: $customMenuId, updateInput: $updateInput) {
    _id
    serviceKey
    customMenuKey
    parentCustomMenuId
    parentCustomMenuKey
    customMenuPath
    level
    title
    multilingualTitle
    description
    # ...
  }
}
```

`updateInput`: `UpdateCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| serviceKey | `String` |  | serviceKey ใช้เพื่อบอกว่า เป็น ของ service ไหน |
| title | `String` |  | ชื่อที่แสดงบนเมนู |
| multilingualTitle | `JSON` |  | ชื่อที่แสดงบนเมนู (หลายภาษา) |
| description | `String` |  | คำอธิบายสำหรับผู้ดูแลระบบ |
| icon | `String` |  | ไอคอน (เช่นชื่อ material icon) |
| type | `EnumCustomMenuType` |  | type ของเมนู |
| templatePath | `String` |  | Path ที่มี dynamic param ได้ เช่น /org/{orgKey}/edit/{itemId} |
| pathParams | `[String]` |  | รายชื่อ param ใน path |
| queryParams | `[String]` |  | รายชื่อ query string param ที่ต้องแนบ (ถ้าต้องการ) |
| url | `String` |  | สำหรับ external-link หรือ iframe |
| openInNewTab | `Boolean` |  | สำหรับ external-link |
| fileName | `String` |  | สำหรับ external-download |
| priority | `Int` |  | ใช้ตัดสินใจเมื่อ route ตรงกับหลายอัน (ยิ่งน้อยยิ่งสำคัญกว่า) |
| order | `Int` |  | ลำดับแสดงผลบนหน้าจอ (UI เท่านั้น, ไม่มีผลกับ route) |
| showBadgeCount | `Boolean` |  | ใช้แสดง badge count |
| badgeCountKey | `String` |  | Key ใช้ดึงค่าจำนวน badge |
| breadCrumbs | `[CreateBreadCrumbInput]` |  | breadCrumbs รายชื่อ breadcrumb ที่จะแสดงบนเมนูนี้ |
| isGroupMenu | `Boolean` |  | เป็น group menu หรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isSystem | `Boolean` |  | isSystem เมนูระบบ (ใช้กรองในหน้าแอดมินเท่านั้น) |

argument อื่น: `customMenuId: String!`

Response: `CustomMenu`

---

#### deleteCustomMenu

ลบข้อมูล CustomMenu

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteCustomMenu`

```graphql
mutation DeleteCustomMenu($customMenuId: String!) {
  deleteCustomMenu(customMenuId: $customMenuId) {
    _id
    serviceKey
    customMenuKey
    parentCustomMenuId
    parentCustomMenuKey
    customMenuPath
    level
    title
    multilingualTitle
    description
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| customMenuId | `String!` |

Response: `CustomMenu`

---

#### addCustomMenuToDefaultOrganizationRole

เพิ่ม CustomMenu ลงใน DefaultOrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addCustomMenuToDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddCustomMenuToDefaultOrganizationRole($input: AddCustomMenuToDefaultOrganizationRoleInput!) {
  addCustomMenuToDefaultOrganizationRole(input: $input)
}
```

`input`: `AddCustomMenuToDefaultOrganizationRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| defaultOrganizationRoleIds | `[String!]!` | ใช่ | defaultOrganizationRoleIds ใช้เพื่อบอกว่า เป็น id ของ defaultOrganizationRole ไหน |
| customMenuSelectType | `EnumSelect` |  | ประเภทการเลือก ALL เลือกทั้งหมด, SELECT เลือกบางส่วน |
| customMenuIds | `[String]` |  | customMenuIds ใช้เพื่อบอกว่า เป็น id ของ customMenu ไหน |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `Boolean!`

---

#### removeCustomMenuFromDefaultOrganizationRole

ลบ CustomMenu จาก DefaultOrganizationRole

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeCustomMenuFromDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemoveCustomMenuFromDefaultOrganizationRole($defaultOrganizationRoleCustomMenuId: ID!) {
  removeCustomMenuFromDefaultOrganizationRole(defaultOrganizationRoleCustomMenuId: $defaultOrganizationRoleCustomMenuId) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    defaultOrganizationRoleId
    defaultOrganizationRoleKey
    defaultOrganizationRole { _id appKey defaultOrganizationRoleKey title subTitle description }
    systemNote
    priority
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| defaultOrganizationRoleCustomMenuId | `ID!` |

Response: `DefaultOrganizationRoleCustomMenu!`

---

#### updateDefaultOrganizationRoleCustomMenu

แก้ไขข้อมูล DefaultOrganizationRoleCustomMenu ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateDefaultOrganizationRoleCustomMenu`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateDefaultOrganizationRoleCustomMenu($defaultOrganizationRoleCustomMenuId: ID!, $input: UpdateDefaultOrganizationRoleCustomMenuInput!) {
  updateDefaultOrganizationRoleCustomMenu(defaultOrganizationRoleCustomMenuId: $defaultOrganizationRoleCustomMenuId, input: $input) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    defaultOrganizationRoleId
    defaultOrganizationRoleKey
    defaultOrganizationRole { _id appKey defaultOrganizationRoleKey title subTitle description }
    systemNote
    priority
    # ...
  }
}
```

`input`: `UpdateDefaultOrganizationRoleCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| systemNote | `String` |  | ไว้สำหรับ admin |
| priority | `Int` |  | ลำดับความสำคัญ (priority) |
| order | `Int` |  | ลำดับการแสดงผล (order) |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `defaultOrganizationRoleCustomMenuId: ID!`

Response: `DefaultOrganizationRoleCustomMenu!`

---

#### addPermissionToDefaultOrganizationRole

เพิ่ม Permission ลงใน DefaultOrganizationRole ทำข้าม app ได้ โดยเช็กจาก defaultOrganizationRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addPermissionToDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddPermissionToDefaultOrganizationRole($input: AddPermissionToDefaultOrgRoleInput!) {
  addPermissionToDefaultOrganizationRole(input: $input)
}
```

`input`: `AddPermissionToDefaultOrgRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| defaultOrganizationRoleIds | `[String!]!` | ใช่ | defaultOrganizationRoleId ใช้เพื่อบอกว่า เป็น id ของ defaultOrganizationRole ไหน |
| permissionSelectType | `EnumSelect` |  | ประเภทการเลือก ALL เลือกทั้งหมด, SELECT เลือกบางส่วน |
| permissionIds | `[String]` |  | permissionId ใช้เพื่อบอกว่า เป็น id ของ permission ไหน |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `Boolean!`

---

#### removePermissionFromDefaultOrganizationRole

ลบ Permission จาก DefaultOrganizationRole ทำข้าม app ได้ โดยเช็กจาก defaultOrganizationRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removePermissionFromDefaultOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemovePermissionFromDefaultOrganizationRole($defaultOrgRolePermissionId: ID!) {
  removePermissionFromDefaultOrganizationRole(defaultOrgRolePermissionId: $defaultOrgRolePermissionId) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    defaultOrganizationRoleId
    defaultOrganizationRoleKey
    defaultOrganizationRole { _id appKey defaultOrganizationRoleKey title subTitle description }
    systemNote
    isActive
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| defaultOrgRolePermissionId | `ID!` |

Response: `DefaultOrgRolePermission!`

---

#### updateDefaultOrganizationRolePermission

แก้ไขข้อมูล DefaultOrganizationRolePermission ตาม id ทำข้าม app ได้ โดยเช็กจาก defaultOrganizationRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateDefaultOrganizationRolePermission`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateDefaultOrganizationRolePermission($defaultOrgRolePermissionId: ID!, $input: UpdateDefaultOrgRolePermissionInput!) {
  updateDefaultOrganizationRolePermission(defaultOrgRolePermissionId: $defaultOrgRolePermissionId, input: $input) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    defaultOrganizationRoleId
    defaultOrganizationRoleKey
    defaultOrganizationRole { _id appKey defaultOrganizationRoleKey title subTitle description }
    systemNote
    isActive
    # ...
  }
}
```

`input`: `UpdateDefaultOrgRolePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน เมื่อเป็น false จะลบ userPolicy, userDefaultOrgRolePermission เมื่อแก้กลับเป็น true gen userPolicy,userDefaultOrgRolePermission |

argument อื่น: `defaultOrgRolePermissionId: ID!`

Response: `DefaultOrgRolePermission!`

---

#### createInviteCode

สร้าง Invite Code

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createInviteCode`

```graphql
mutation CreateInviteCode($createInput: CreateInviteCodeInput!) {
  createInviteCode(createInput: $createInput) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

`createInput`: `CreateInviteCodeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| inviteCodeKey | `String` |  | key ของ invite code |
| title | `String!` | ใช่ | ชื่อ |
| subTitle | `String` |  | ชื่อรอง |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | system note |
| defaultOrganizationKey | `String` |  | default organization key |
| maxUses | `Int` |  | จำนวนการใช้งานสูงสุด null หมายถึงไม่จำกัด |
| isPublic | `Boolean` |  | เป็น public หรือไม่ |
| validStartTime | `Date` |  | วันที่ invite code มีผล |
| validEndTime | `Date` |  | วันที่ invite code หมดอายุ |
| url | `String` |  | url สำหรับ QR code |
| appRoleIds | `[String]` |  | app role id ที่จะเอามาใช้กับ invite code |
| orgRoleIds | `[String]` |  | org role id ที่จะเอามาใช้กับ invite code |
| isDefault | `Boolean` |  | เป็นค่าเริ่มต้นหรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน ถ้าไม่ใส่ valid start time มา และ isActive เป็น true จะเป็น new Date() |

Response: `InviteCode!`

---

#### updateInviteCode

แก้ไข Invite Code

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateInviteCode`

```graphql
mutation UpdateInviteCode($inviteCodeId: String!, $updateInput: UpdateInviteCodeInput!) {
  updateInviteCode(inviteCodeId: $inviteCodeId, updateInput: $updateInput) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

`updateInput`: `UpdateInviteCodeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| title | `String` |  | ชื่อ |
| subTitle | `String` |  | ชื่อรอง |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | system note |
| defaultOrganizationKey | `String` |  | default organization key |
| maxUses | `Int` |  | จำนวนการใช้งานสูงสุด null หมายถึงไม่จำกัด |
| isPublic | `Boolean` |  | เป็น public หรือไม่ |
| validStartTime | `Date` |  | วันที่ invite code มีผล |
| validEndTime | `Date` |  | วันที่ invite code หมดอายุ |
| url | `String` |  | url สำหรับ QR code |
| appRoleIds | `[String]` |  | app role id ที่จะเอามาใช้กับ invite code **ที่จะเพิ่ม ถ้าจะไม่เพิ่มส่ง [] มา |
| orgRoleIds | `[String]` |  | org role id ที่จะเอามาใช้กับ invite code **ที่จะเพิ่ม ถ้าจะไม่เพิ่มส่ง [] มา |
| isDefault | `Boolean` |  | เป็นค่าเริ่มต้นหรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน ถ้าเป็น true valid start time จะเป็น new Date() |

argument อื่น: `inviteCodeId: String!`

Response: `InviteCode!`

---

#### deleteInviteCode

ลบ Invite Code

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deleteInviteCode`

```graphql
mutation DeleteInviteCode($inviteCodeId: String!) {
  deleteInviteCode(inviteCodeId: $inviteCodeId) {
    _id
    inviteCodeKey
    title
    subTitle
    description
    systemNote
    defaultOrganizationId
    defaultOrganizationKey
    maxUses
    currentUses
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| inviteCodeId | `String!` |

Response: `InviteCode!`

---

#### addCustomMenuToOrganizationRole

เพิ่ม CustomMenu ลงใน OrgRole ทำข้าม app ได้ โดยเช็กจาก orgRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addCustomMenuToOrganizationRole`

```graphql
mutation AddCustomMenuToOrganizationRole($input: AddCustomMenuToOrgRoleInput) {
  addCustomMenuToOrganizationRole(input: $input)
}
```

`input`: `AddCustomMenuToOrgRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| customMenuIds | `[String]` |  | customMenuId ใช้เพื่อบอกว่า เป็น id ของ customMenu ไหน |
| organizationRoleIds | `[String]` |  | organizationRoleId ใช้เพื่อบอกว่า เป็น id ของ orgRole ไหน |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isChildOrganizationAccess | `Boolean` |  | isChildOrganizationAccess ใช้บอกว่าสามารถเข้าถึงข้อมูลของ child organization ได้หรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `Boolean`

---

#### removeCustomMenuFromOrganizationRole

ลบ CustomMenu จาก OrgRole ทำข้าม app ได้ โดยเช็กจาก orgRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeCustomMenuFromOrganizationRole`

```graphql
mutation RemoveCustomMenuFromOrganizationRole($orgRolesCustomMenuId: String) {
  removeCustomMenuFromOrganizationRole(orgRolesCustomMenuId: $orgRolesCustomMenuId) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    organizationId
    organizationKey
    organizationPath
    organizationRoleId
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| orgRolesCustomMenuId | `String` |

Response: `OrgRolesCustomMenu`

---

#### updateOrganizationRoleCustomMenu

แก้ไขข้อมูล OrgRolesCustomMenu ตาม id ทำข้าม app ได้ โดยเช็กจาก orgRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationRoleCustomMenu`

```graphql
mutation UpdateOrganizationRoleCustomMenu($orgRolesCustomMenuId: String, $input: UpdateOrgRoleCustomMenuInput) {
  updateOrganizationRoleCustomMenu(orgRolesCustomMenuId: $orgRolesCustomMenuId, input: $input) {
    _id
    appKey
    customMenuId
    customMenuKey
    customMenuPath
    customMenu { _id serviceKey customMenuKey parentCustomMenuId parentCustomMenuKey customMenuPath }
    organizationId
    organizationKey
    organizationPath
    organizationRoleId
    # ...
  }
}
```

`input`: `UpdateOrgRoleCustomMenuInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isChildOrganizationAccess | `Boolean` |  | isChildOrganizationAccess ใช้บอกว่าสามารถเข้าถึงข้อมูลของ child organization ได้หรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

argument อื่น: `orgRolesCustomMenuId: String`

Response: `OrgRolesCustomMenu`

---

#### addPermissionToOrganizationRole

เพิ่ม Permission ลงใน OrgRole ทำข้าม app ได้ โดยเช็กจาก orgRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addPermissionToOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddPermissionToOrganizationRole($input: AddPermissionToOrgRoleInput!) {
  addPermissionToOrganizationRole(input: $input)
}
```

`input`: `AddPermissionToOrgRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| permissionId | `String` |  | permissionId ใช้เพื่อบอกว่า เป็น id ของ permission ไหน |
| organizationRoleId | `String` |  | organizationRoleId ใช้เพื่อบอกว่า เป็น id ของ orgRole ไหน |
| permissionIds | `[String]` |  | permissionIds สำหรับใช้ในการระบุ List Permission ที่ต้องการให้ OrganizationRole ใช้งาน |
| organizationRoleIds | `[String]` |  | organizationRoleIds สำหรับใช้ในการระบุ List AppRole ที่ต้องการให้ใช้งาน permission |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |
| isChildOrganizationAccess | `Boolean` |  | สามารถเข้าถึงข้อมูลของ child organization ได้หรือไม่ |

Response: `Boolean!`

---

#### removePermissionFromOrganizationRole

ลบ Permission จาก OrgRole ทำข้าม app ได้ โดยเช็กจาก orgRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removePermissionFromOrganizationRole`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemovePermissionFromOrganizationRole($orgRolesPermissionId: ID!) {
  removePermissionFromOrganizationRole(orgRolesPermissionId: $orgRolesPermissionId) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    organizationId
    organizationKey
    organizationPath
    organizationRoleId
    organizationRoleKey
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| orgRolesPermissionId | `ID!` |

Response: `OrgRolePermission!`

---

#### updateOrganizationRolePermission

แก้ไขข้อมูล OrgRolePermission ตาม id ทำข้าม app ได้ โดยเช็กจาก orgRoleId ถ้ามีสิทธิ์ `systemApp`

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateOrganizationRolePermission`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateOrganizationRolePermission($orgRolesPermissionId: ID!, $input: UpdateOrgRolePermissionInput!) {
  updateOrganizationRolePermission(orgRolesPermissionId: $orgRolesPermissionId, input: $input) {
    _id
    appKey
    permissionId
    permissionKey
    permission { _id serviceKey permissionKey title description systemNote }
    organizationId
    organizationKey
    organizationPath
    organizationRoleId
    organizationRoleKey
    # ...
  }
}
```

`input`: `UpdateOrgRolePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isChildOrganizationAccess | `Boolean` |  | สามารถเข้าถึงข้อมูลของ child organization ได้หรือไม่ |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน เมื่อเป็น false จะลบ userPolicy, userOrgRolePermission เมื่อแก้กลับเป็น true gen userPolicy,userOrgRolePermission |

argument อื่น: `orgRolesPermissionId: ID!`

Response: `OrgRolePermission!`

---

#### createPermission

สร้างข้อมูล Permission

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `createPermission`

```graphql
mutation CreatePermission($createInput: CreatePermissionInput!) {
  createPermission(createInput: $createInput) {
    _id
    serviceKey
    permissionKey
    title
    description
    systemNote
    isSystem
    isGenerateApplication
    isGenerateOrganization
    appUserPolicyKeyPattern
    # ...
  }
}
```

`createInput`: `CreatePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| serviceKey | `String!` | ใช่ | serviceKey ใช้เพื่อบอกว่า เป็น ของ service ไหน |
| permissionKey | `String!` | ใช่ | permissionKey ใช้เป็น key ในการระบุ userPolicyKey |
| title | `String` |  | ชื่อ |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isSystem | `Boolean` |  | isSystem |
| isGenerateApplication | `Boolean` |  | ถ้าเป็น true ถึงจะสามารถผูกได้กับ appRoles (ไว้เช็กสิทธิ lv app) |
| isGenerateOrganization | `Boolean` |  | ถ้าเป็น true ถึงจะสามารถผูกได้กับ organizationRoles (ไว้เช็กสิทธิ lv org) |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน |

Response: `Permission!`

---

#### updatePermission

แก้ไขข้อมูล Permission

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updatePermission`

```graphql
mutation UpdatePermission($permissionId: ID!, $updateInput: UpdatePermissionInput!) {
  updatePermission(permissionId: $permissionId, updateInput: $updateInput) {
    _id
    serviceKey
    permissionKey
    title
    description
    systemNote
    isSystem
    isGenerateApplication
    isGenerateOrganization
    appUserPolicyKeyPattern
    # ...
  }
}
```

`updateInput`: `UpdatePermissionInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| title | `String` |  | ชื่อ |
| description | `String` |  | รายล่ะเอียด |
| systemNote | `String` |  | ไว้สำหรับ admin |
| isSystem | `Boolean` |  | isSystem |
| isActive | `Boolean` |  | สถานะการใช้งาน true เมื่อเปิดการใช้งาน เมื่อเป็น false จะทำการปิดการใช้งาน appRolesPermission,organizationRolesPermission และ ลบ userPolicy, userPermission เมื่อแก้กลับเป็น true ต้องไปแก้ใน appRolesPermission,organizationRolesPermission ซ้ำเพื่อเปิดให้เจน userPolicy,userPermission เนื่องจาก ป้องการการพลาดให้สิทธิที่ไม่ควรให้กับ user |

argument อื่น: `permissionId: ID!`

Response: `Permission!`

---

#### deletePermission

ลบข้อมูล Permission

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `deletePermission`

```graphql
mutation DeletePermission($permissionId: ID!) {
  deletePermission(permissionId: $permissionId) {
    _id
    serviceKey
    permissionKey
    title
    description
    systemNote
    isSystem
    isGenerateApplication
    isGenerateOrganization
    appUserPolicyKeyPattern
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| permissionId | `ID!` |

Response: `Permission!`

---

#### addAppRoleToUser

เพิ่ม AppRole ให้กับ User ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addAppRoleToUser`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddAppRoleToUser($addAppRoleToUserInput: AddAppRoleToUserInput!) {
  addAppRoleToUser(addAppRoleToUserInput: $addAppRoleToUserInput)
}
```

`addAppRoleToUserInput`: `AddAppRoleToUserInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String!` | ใช่ | ข้อมูล AppKey สำหรับใช้ในการระบุ App ที่ต้องการให้ User ใช้งาน |
| authId | `String` |  | ข้อมูล AuthId สำหรับใช้ในการระบุ User ที่ต้องการให้ใช้งาน AppRole |
| appRoleId | `String` |  | ข้อมูล AppRoleId สำหรับใช้ในการระบุ AppRole ที่ต้องการให้ User ใช้งาน |
| authIds | `[String]` |  | authIds สำหรับใช้ในการระบุ List User ที่ต้องการให้ใช้งาน AppRole |
| appRoleIds | `[String]` |  | appRoleIds สำหรับใช้ในการระบุ List AppRole ที่ต้องการให้ User ใช้งาน |
| isActive | `Boolean` |  | ระบุสถานะของการใช้งานของ AppRole นี้ |

Response: `Boolean!`

---

#### removeAppRoleFromUser

ลบ AppRole ให้กับ User ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeAppRoleFromUser`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemoveAppRoleFromUser($userAppRoleId: ID!, $appKey: String) {
  removeAppRoleFromUser(userAppRoleId: $userAppRoleId, appKey: $appKey) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    isActive
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| userAppRoleId | `ID!` |
| appKey | `String` |

Response: `UserAppRole!`

---

#### updateUserAppRole

อัปเดต AppRole ให้กับ User ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateUserAppRole`

```graphql
mutation UpdateUserAppRole($updateUserAppRoleInput: [UpdateUserAppRoleInput]!) {
  updateUserAppRole(updateUserAppRoleInput: $updateUserAppRoleInput) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    appRoleId
    appRoleKey
    appRole { _id appRoleKey title subTitle description systemNote }
    isActive
    createdAt
    updatedAt
  }
}
```

`updateUserAppRoleInput`: `UpdateUserAppRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String!` | ใช่ | ข้อมูล AppKey สำหรับใช้ในการระบุ App ที่ต้องการให้ User ใช้งาน |
| authId | `String!` | ใช่ | ข้อมูล AuthId สำหรับใช้ในการระบุ User ที่ต้องการให้ใช้งาน AppRole |
| appRoleId | `String!` | ใช่ | ข้อมูล AppRoleId สำหรับใช้ในการระบุ AppRole ที่ต้องการให้ User ใช้งาน |
| isActive | `Boolean!` | ใช่ | ระบุสถานะของการใช้งานของ AppRole นี้ |

Response: `[UserAppRole]!`

---

#### addOrganizationRoleToUser

เพิ่ม OrganizationRole ให้กับ User ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `addOrganizationRoleToUser`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddOrganizationRoleToUser($addOrganizationRoleToUserInput: AddOrganizationRoleToUserInput!) {
  addOrganizationRoleToUser(addOrganizationRoleToUserInput: $addOrganizationRoleToUserInput)
}
```

`addOrganizationRoleToUserInput`: `AddOrganizationRoleToUserInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String!` | ใช่ | ข้อมูล AppKey สำหรับใช้ในการระบุ App ที่ต้องการให้ User ใช้งาน |
| authId | `String` |  | ข้อมูล AuthId สำหรับใช้ในการระบุ User ที่ต้องการให้ใช้งาน OrgRole |
| organizationRoleId | `String` |  | ข้อมูล OrganizationRoleId สำหรับใช้ในการระบุ OrgRole ที่ต้องการให้ User ใช้งาน |
| authIds | `[String]` |  | authIds สำหรับใช้ในการระบุ List User ที่ต้องการให้ใช้งาน AppRole |
| organizationRoleIds | `[String]` |  | OrganizationRoleIds สำหรับใช้ในการระบุ List OrganizationRole ที่ต้องการให้ User ใช้งาน |
| isActive | `Boolean` |  | ระบุสถานะของการใช้งานของ OrgRole นี้ |

Response: `Boolean!`

---

#### removeOrgRoleFromUser

ลบ OrgRole ให้กับ User ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeOrgRoleFromUser`

```graphql
mutation RemoveOrgRoleFromUser($userOrgRoleId: ID!) {
  removeOrgRoleFromUser(userOrgRoleId: $userOrgRoleId) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    organizationRoleKey
    organizationRole { _id organizationKey organizationRoleKey title subTitle description }
    organizationKey
    organizationPath
    isActive
    createdAt
    updatedAt
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| userOrgRoleId | `ID!` |

Response: `UserOrgRole!`

---

#### updateUserOrgRole

อัปเดต OrgRole ให้กับ User ในการใช้งานระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateUserOrgRole`

```graphql
mutation UpdateUserOrgRole($updateUserOrgRoleInput: [UpdateUserOrgRoleInput]!) {
  updateUserOrgRole(updateUserOrgRoleInput: $updateUserOrgRoleInput) {
    _id
    authId
    profile { _id appKey firstName middleName lastName displayName }
    organizationRoleKey
    organizationRole { _id organizationKey organizationRoleKey title subTitle description }
    organizationKey
    organizationPath
    isActive
    createdAt
    updatedAt
    # ...
  }
}
```

`updateUserOrgRoleInput`: `UpdateUserOrgRoleInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String!` | ใช่ | ข้อมูล AppKey สำหรับใช้ในการระบุ App ที่ต้องการให้ User ใช้งาน |
| authId | `String!` | ใช่ | ข้อมูล AuthId สำหรับใช้ในการระบุ User ที่ต้องการให้ใช้งาน OrgRole |
| organizationRoleId | `String!` | ใช่ | ข้อมูล organizationRoleId สำหรับใช้ในการระบุ OrgRole ที่ต้องการให้ User ใช้งาน |
| isActive | `Boolean!` | ใช่ | ระบุสถานะของการใช้งานของ OrgRole นี้ |

Response: `[UserOrgRole]!`

---

#### reCalculateUserPolicy

ทำการคำนวณ UserPolicy ใหม่ โดยจะคำนวณจาก UserPermission ทั้งหมด และสร้าง UserPolicy ใหม่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `reCalculateUserPolicy`

```graphql
mutation ReCalculateUserPolicy($appKey: String!) {
  reCalculateUserPolicy(appKey: $appKey)
}
```

| argument | Type |
| --- | --- |
| appKey | `String!` |

Response: `Boolean!`

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
| `refresh-data` | core สั่งให้ส่งข้อมูลที่ถืออยู่ขึ้นไปใหม่ + ล้าง cache · ACL จะส่ง `sync-permission` และ `sync-invite-code` และดึงเมนูของหน้าบ้านใหม่ |
| `sync-app-certificate` | รับ AppCertificate ของ service ต่อแอป จาก core |
| `sync-app-credential` | รับ AppCredential ของแอป จาก Authentication Service ใช้ตรวจ token / header |
| `sync-service-setting` / `sync-app-service-setting` | รับค่าตั้งค่าเพิ่มเติมแบบ JSON ทั้งระบบ / รายแอป |
| `sync-application` | รับข้อมูลแอป |

---

### sync-permission

รับ permission จากทุก service (รายละเอียด payload ดู [ลำดับการทำงานของสิทธิ์](#permission-flow))

    topic: sync-permission

---

### sync-service

รับทะเบียน service จาก core · ถ้า service เป็นหน้าบ้าน ACL จะดึงรายการเมนูจาก `urlGetMetaData` มาสร้าง Custom Menu

    topic: sync-service

| key | Type | คำอธิบาย |
| --- | --- | --- |
| service.serviceKey | string | serviceKey |
| service.name / description / version | string | ข้อมูล service |
| service.isCoreSet | boolean | เป็น service ใน core set |
| service.isActive | boolean | เปิดใช้งาน |
| service.type | string | `BACKEND`, `MAIN_FRONTEND`, `MICRO_FRONTEND`, `OTHER` |
| service.urlFrontend | string | URL ของหน้าบ้าน |
| service.urlGetMetaData | string | URL ที่คืนรายการเมนูของหน้าบ้าน |

---

### sync-app-role / sync-organization-role / sync-default-organization-role

รับนิยาม role จาก [Unit Service](unitService.md) เพื่อใช้ผูก permission / เมนู / ผู้ใช้ · เมื่อได้ organizationRole ที่สร้างจาก defaultOrganizationRole ACL จะคัดลอก permission และเมนูของแม่แบบให้อัตโนมัติ

    topic: sync-app-role
    topic: sync-organization-role
    topic: sync-default-organization-role

| key | คำอธิบาย |
| --- | --- |
| appRole | `id`, `appKey`, `appRoleKey`, `title`, `subTitle`, `description`, `isInvite`, `isActive`, ... (`isAdmin: true` สำหรับ role admin ที่สร้างตอนเพิ่มแอป) |
| organizationRole | `id`, `appKey`, `organizationId`, `organizationKey`, `organizationPath`, `organizationRoleKey`, `title`, `isInvite`, `isActive`, ... (`defaultOrganizationRoleId` / `defaultOrganizationRoleKey` เมื่อสร้างจากแม่แบบ) |
| defaultOrganizationRole | `id`, `appKey`, `defaultOrganizationRoleKey`, `title`, `isActive`, ... |
| action | `ADD`, `REMOVE` |

---

### sync-organization

รับข้อมูลองค์กรจาก [Unit Service](unitService.md) (ใช้โครงต้นไม้ `organizationPath` ในการแตกสิทธิ์ลงองค์กรลูก)

    topic: sync-organization

---

### sync-auth

รับข้อมูลบัญชีใหม่จาก [Authentication Service](authenticationService.md) · ถ้าสมัครด้วย invite code จะผูก appRole / organizationRole ตาม invite code ให้ผู้ใช้อัตโนมัติ

    topic: sync-auth

---

### sync-profile

รับข้อมูลโปรไฟล์จาก [Profile Service](userService.md) ใช้แสดงคู่กับ role (`getProfileWithAppRoles`, `getProfileWithOrgRoles`)

    topic: sync-profile

---

### set-user-role

ให้ service อื่นกำหนด role ให้ผู้ใช้ผ่าน Kafka

    topic: set-user-role

| key | Type | คำอธิบาย |
| --- | --- | --- |
| userRole.authId | string | ผู้ใช้ |
| userRole.appKey | string | appKey |
| userRole.appRoleIdList | string[] | appRole ที่ต้องการให้ |
| userRole.orgRoleIdList | string[] | organizationRole ที่ต้องการให้ |
| action | string | `ADD` |

---

### schedule-alarm

รับการแจ้งเตือนตามเวลาจาก Schedule Service เพื่อเปิด / ปิด invite code ตาม `validStartTime` / `validEndTime` แล้วตอบผลทาง `schedule-alarm-result`

    topic: schedule-alarm

---

### sync-generate-user-policy / sync-generate-user-custom-menu

คิวงานภายในของ ACL เอง (ACL ส่งให้ตัวเอง) ใช้คำนวณ UserPolicy และเมนูของผู้ใช้หลังมีการเปลี่ยน role / permission / เมนู

    topic: sync-generate-user-policy
    topic: sync-generate-user-custom-menu

---

<br>
<br>

## Kafka Produce Reference

---

### sync-user-policy

ส่ง UserPolicy ให้ service เจ้าของ permission · header `serviceKey` = service ปลายทาง (service ต้องรับเฉพาะข้อความที่ `serviceKey` ตรงกับตัวเอง)

    topic: sync-user-policy

| key | Type | คำอธิบาย |
| --- | --- | --- |
| userPolicy._id | string | id |
| userPolicy.appKey | string | appKey |
| userPolicy.userPolicyKey | string | key ตามรูปแบบใน [ลำดับการทำงานของสิทธิ์](#permission-flow) |
| userPolicy.serviceKey | string | service เจ้าของ permission |
| userPolicy.permissionKey | string | permission |
| userPolicy.authId | string | ผู้ใช้ |
| userPolicy.organizationId | string | องค์กร (เฉพาะระดับองค์กร) |
| action | string | ดูตารางด้านล่าง |

| action | ข้อมูลที่ส่งมา | ความหมาย |
| --- | --- | --- |
| `ADD` | `userPolicy` | เพิ่ม UserPolicy |
| `REMOVE` | `userPolicy` | ลบ UserPolicy ตัวนั้น |
| `REMOVE_APP` | `appKey` | ลบ UserPolicy ทั้งหมดของแอป |
| `REMOVE_PERMISSION` | `permissionKey` | ลบ UserPolicy ทั้งหมดของ permission นั้น |
| `REMOVE_USER` | `authId`, `appKey` | ลบ UserPolicy ทั้งหมดของผู้ใช้ในแอป |
| `REMOVE_ORGANIZATION` | `oranizationId` (สะกดตามโค้ด), `appKey` | ลบ UserPolicy ทั้งหมดขององค์กร |

---

### sync-invite-code

ส่งข้อมูล invite code ให้ [Authentication Service](authenticationService.md) ใช้ตอนสมัคร

    topic: sync-invite-code

| key | Type | คำอธิบาย |
| --- | --- | --- |
| inviteCode.id / inviteCodeKey | string | id และรหัสเชิญ |
| inviteCode.title / subTitle / description | string | ข้อมูลแสดงผล |
| inviteCode.defaultOrganizationId / defaultOrganizationKey | string | องค์กรเริ่มต้น |
| inviteCode.maxUses / currentUses | number | จำนวนครั้งที่ใช้ได้ / ใช้ไปแล้ว |
| inviteCode.isPublic / isDefault / isActive | boolean | สถานะ |
| inviteCode.validStartTime / validEndTime | Date | ช่วงเวลาที่ใช้ได้ |
| inviteCode.appRoleIds / orgRoleIds | string[] | role ที่จะได้รับ |
| action | string | `ADD`, `REMOVE` |

---

### set-schedule / schedule-alarm-result

ลงทะเบียนนัดกับ Schedule Service สำหรับเวลาเริ่ม / หมดอายุของ invite code (`schedule.scheduleRefKey` = `<inviteCodeKey>:valid` หรือ `<inviteCodeKey>:expired`) และตอบผลหลังได้ `schedule-alarm`

    topic: set-schedule
    topic: schedule-alarm-result

---

### create-notification

ส่งคำขอแจ้งเตือนไปที่ Notification Service

    topic: create-notification

---

### sync-permission

ส่ง permission ของ `access-control` เอง (ตอนได้ `refresh-data`)

    topic: sync-permission

---

### sync-app-role-permission / sync-organization-role-permission

ประกาศการผูก permission กับ appRole / organizationRole เมื่อมีการเปลี่ยนแปลง (สำหรับ service ที่ต้องการติดตาม)

    topic: sync-app-role-permission
    topic: sync-organization-role-permission

---

### sync-generate-user-policy / sync-generate-user-custom-menu

คิวงานภายใน (ดู consume ด้านบน)

---

> อัปเดตจากโค้ด gumon-access-control-service@2bca76f · 2026-10-05
