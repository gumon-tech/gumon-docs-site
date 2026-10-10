# Authentication Service

Service สำหรับยืนยันตัวตนผู้ใช้ เป็นเจ้าของบัญชีผู้ใช้ (แยกตามแอป), การสมัครและ login ทุกแบบ, session และ token, AppCredential (ตัวตนของแอปที่ใช้เรียก API) และข้อมูลอุปกรณ์มือถือ / push token

    serviceKey: authentication

- บัญชีผูกกับแอป (`appKey`) — คนเดียวกันใช้ 2 แอป จะมี 2 บัญชี
- ข้อมูลชื่อ-นามสกุลและโปรไฟล์อยู่ที่ [Profile Service](userService.md) โดย authentication ส่งค่าเริ่มต้นไปให้ผ่าน topic `sync-auth`
- สิทธิ์การใช้งาน (role / permission) อยู่ที่ [ACL Service](aclService.md)

<br>

- [วิธี login ที่มี](#login-methods)
- [AppCredential และ header ที่ต้องส่ง](#app-credential)
- [API Reference](#api-reference)
- [kafka consume Reference](#kafka-consume-reference)
- [Kafka Produce Reference](#kafka-produce-reference)

---

<a id="login-methods"></a>

## วิธี login ที่มี

ทุกวิธีคืน token ให้โดยตรง (ไม่มี authorization-code flow)

| วิธี | API | ผลลัพธ์ |
| --- | --- | --- |
| username / email / เบอร์โทร + password | `loginWithAccessType`, `loginWithUserNameAccessType`, `loginWithEmailAccessType`, `loginWithPhoneNumberAccessType` | ได้ `token { accessToken refreshToken }` ทันที |
| WebSocket | `getLoginSocketId` แล้ว `loginWithUserNameSocketType` / `loginWithEmailSocketType` / `loginWithPhoneNumberSocketType` | token ถูกส่งเข้า socket ที่ได้จาก `getLoginSocketId` (เหมาะกับหน้า login ที่แยกจากแอปปลายทาง) |
| OTP ทาง email / SMS | `loginWithEmailOtpType` / `loginWithPhoneNumberOtpType` → `verifierOtp` | ขั้นแรกได้ `otpRef` แล้วยืนยัน OTP เพื่อรับ token |
| Google | `loginWithGoogleOAuth`, `registerWithGoogleOAuth`, `linkAccountWithGoogleOAuth`, `unlinkAccountFromGoogleOAuth` | ส่ง `id_token` ของ Google |
| Facebook | `loginWithFaceBookOAuth`, `registerWithFaceBookOAuth`, `linkAccountWithFaceBookOAuth`, `unlinkAccountFromFaceBookOAuth` | ส่ง `access_token` ของ Facebook |
| Apple | `loginWithAppleOAuth`, `registerWithAppleOAuth`, `linkAccountWithAppleOAuth`, `unlinkAccountFromAppleOAuth` | ส่ง `id_token` ของ Apple |

- ลืมรหัสผ่าน: `resetPasswordWithEmail` / `resetPasswordWithPhoneNumber` → `verifierOtp` → `resetMyPasswordByOtpVerifierRef`
- ต่ออายุ token: `refreshToken` (ได้ refreshToken ชุดใหม่ทุกครั้ง ชุดเดิมใช้ซ้ำไม่ได้) · ออกจากระบบ: `logout`, `logoutAll`
- การ login ด้วย Google / Facebook / Apple ต้องตั้งค่า provider ต่อแอปก่อน ผ่าน App Service Setting ของ service `authentication` (topic `sync-app-service-setting`) ในรูป

```json
{
  "googleOAuth":   { "clientId": "<client id ของ Google, คั่นหลายค่าด้วย , ได้>" },
  "facebookOAuth": { "appId": "<app id ของ Facebook>", "appSecret": "<app secret ของ Facebook>" },
  "appleOAuth":    { "clientId": "<client id ของ Apple, คั่นหลายค่าด้วย , ได้>" }
}
```

---

<a id="app-credential"></a>

## AppCredential และ header ที่ต้องส่ง

AppCredential คือตัวตนของ "ผู้เรียก API" ในแต่ละแอป สร้างด้วย `generateAppCredential` แต่ละตัวมี `clientId` และตั้งกฎได้ เช่น host ที่อนุญาต (`authorizedHosts`), redirect URL ที่อนุญาต (`authorizedRedirectUrls`), อายุ token (`jwtAccessExpireTime`, `jwtRefreshExpireTime` หน่วยวินาที), การล็อกบัญชีเมื่อ login ผิด (`numberOfFail`, `minutesTimeFail`, `minutesTimeLock`) และวันหมดอายุ (`expiryAt`)

AppCredential มี 2 ประเภท (`appCredentialType`)

| ประเภท | ใช้กับ | ข้อมูลที่ใช้ |
| --- | --- | --- |
| `USER` | หน้าบ้าน (เว็บ / แอปมือถือ) ที่ผู้ใช้ login | `clientId` |
| `SYSTEM` | ระบบภายนอก / server-to-server (ที่มักเรียกกันว่า **apiKey**) | `clientId` + `clientSecret` · `clientSecret` แสดงครั้งเดียวตอน `generateAppCredential` ถ้าทำหายต้องสร้างใหม่ |

header ที่ service ทุกตัวใน Gumon ใช้ตรวจตัวตน (ตรวจได้เองในแต่ละ service เพราะ AppCredential ถูกส่งไปให้ทุก service ผ่าน topic `sync-app-credential`)

| ผู้เรียก | header |
| --- | --- |
| ผู้ใช้ที่ login แล้ว | `authorization: <accessToken>` (+ `X-APP-CLIENT-ID` ได้ ถ้าส่งต้องตรงกับ clientId ใน token) |
| ระบบภายนอก (SYSTEM) | `X-APP-CLIENT-ID: <clientId>` + `X-APP-CLIENT-SECRET: <clientSecret>` |
| ระดับแอป ไม่ต้อง login (เช่น login / register) | `X-APP-CLIENT-ID: <clientId>` |

ใน API Reference ด้านล่าง "การยืนยันตัวตน" บอกว่า API นั้นต้องใช้ header แบบไหน และ "สิทธิ์" คือ permissionKey ของ service `authentication` ที่ผู้เรียกต้องได้รับผ่าน role ใน [ACL Service](aclService.md)

---

<br>
<br>

## API Reference

---

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

---

#### getMySession

ดึงข้อมูล session ปัจจุบันของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetMySession {
  getMySession {
    _id
    authId
    appCredentialId
    appCredential { _id appKey clientId description validateAuthorizedHosts validateAuthorizedRedirectUrls }
    clientId
    host
    ipAddress
    userAgent
    cookie
    isActive
    # ...
  }
}
```

Response: `AccountSession`

---

#### getMyAllSessions

ดึงข้อมูล Session ทั้งหมดของผู้ใช้ปัจจุบัน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getMyAllSessions`

```graphql
query GetMyAllSessions($getInput: GetAccountSessionByAuthIdInput) {
  getMyAllSessions(getInput: $getInput) {
    sessions { _id authId appCredentialId clientId host ipAddress }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetAccountSessionByAuthIdInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAccountSessionByAuthIdFilterInput` |  |  |
| search | `GetAccountSessionByAuthIdSearchInput` |  |  |
| sort | `GetAccountSessionByAuthIdSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `AccountSessionPagination`

---

#### getSessionByAuthId

ดึงรายการ session ของผู้ใช้ตาม authId

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getSessionByAuthId`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetSessionByAuthId($authId: String!, $getInput: GetAccountSessionByAuthIdInput, $appKey: String) {
  getSessionByAuthId(authId: $authId, getInput: $getInput, appKey: $appKey) {
    sessions { _id authId appCredentialId clientId host ipAddress }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetAccountSessionByAuthIdInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAccountSessionByAuthIdFilterInput` |  |  |
| search | `GetAccountSessionByAuthIdSearchInput` |  |  |
| sort | `GetAccountSessionByAuthIdSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `authId: String!`, `appKey: String`

Response: `AccountSessionPagination`

---

#### getAppCredentials

ดึงข้อมูล AppCredentials ทั้งหมดที่มีในระบบ สามารถจัดการข้าม app ได้

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppCredentials`

```graphql
query GetAppCredentials($input: GetAppCredentialsInput) {
  getAppCredentials(input: $input) {
    appCredentials { _id appKey clientId description validateAuthorizedHosts validateAuthorizedRedirectUrls }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetAppCredentialsInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAppCredentialsFilterInput` |  |  |
| search | `GetAppCredentialsSearchInput` |  |  |
| sort | `GetAppCredentialsSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `AppCredentialPagination!`

---

#### getAppCredentialById

ดึงข้อมูล AppCredentials ตาม id

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppCredentialById`

```graphql
query GetAppCredentialById($appCredentialId: ID!) {
  getAppCredentialById(appCredentialId: $appCredentialId) {
    _id
    appKey
    clientId
    description
    authorizedHosts { _id hostName description isActive }
    validateAuthorizedHosts
    authorizedRedirectUrls { _id redirectUrl description isActive }
    validateAuthorizedRedirectUrls
    minutesTimeLock
    numberOfFail
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appCredentialId | `ID!` |

Response: `AppCredential!`

---

#### getAppCredentialByClientId

ดึงข้อมูล AppCredentials ตาม ClientId

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getAppCredentialByClientId`

```graphql
query GetAppCredentialByClientId($clientId: String!) {
  getAppCredentialByClientId(clientId: $clientId) {
    _id
    appKey
    clientId
    description
    authorizedHosts { _id hostName description isActive }
    validateAuthorizedHosts
    authorizedRedirectUrls { _id redirectUrl description isActive }
    validateAuthorizedRedirectUrls
    minutesTimeLock
    numberOfFail
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| clientId | `String!` |

Response: `AppCredential!`

---

#### checkMobileAppUpdate

เช็กว่าแอปมือถือต้องอัปเดตหรือไม่ จาก clientId, platform และ appVersion

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query CheckMobileAppUpdate($input: CheckMobileAppUpdateInput!) {
  checkMobileAppUpdate(input: $input) {
    clientId
    platform
    appVersion
    minimumAppVersion
    shouldUpdate
    forceUpdate
    appUpdateUrl
  }
}
```

`input`: `CheckMobileAppUpdateInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| clientId | `String!` | ใช่ |  |
| platform | `EnumMobilePlatform!` | ใช่ |  |
| appVersion | `String!` | ใช่ |  |

Response: `CheckMobileAppUpdateResult!`

---

#### getLoginSocketId

ขอ socketId สำหรับ login แบบ WebSocket (ใช้คู่กับ `loginWith*SocketType`)

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetLoginSocketId($redirectURL: String, $socketId: String) {
  getLoginSocketId(redirectURL: $redirectURL, socketId: $socketId) {
    socketId
    redirectURL
  }
}
```

| argument | Type |
| --- | --- |
| redirectURL | `String` |
| socketId | `String` |

Response: `GetLoginSocketIdType!`

---

#### getMyAccount

ดึงข้อมูลบัญชีของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyAccount {
  getMyAccount {
    authId
    username
    emails
    phoneNumbers { countryCode phoneNumber }
    oauthProviders { provider providerSubject linkedAt additionalFields }
    defaultOrganizationKey
    isActive
  }
}
```

Response: `AccountLogin`

---

#### getAccountByOAuthProvider

ค้นบัญชีจาก OAuth provider (`GOOGLE`, `FACEBOOK`, `APPLE`) และ subject ของ provider

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetAccountByOAuthProvider($provider: String!, $providerSubject: String!) {
  getAccountByOAuthProvider(provider: $provider, providerSubject: $providerSubject) {
    authId
    username
    emails
    phoneNumbers { countryCode phoneNumber }
    oauthProviders { provider providerSubject linkedAt additionalFields }
    defaultOrganizationKey
    isActive
  }
}
```

| argument | Type |
| --- | --- |
| provider | `String!` |
| providerSubject | `String!` |

Response: `AccountLogin`

---

#### getAccount

ค้นบัญชีตามเงื่อนไขใน input

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetAccount($getAccountInput: GetAccountInput) {
  getAccount(getAccountInput: $getAccountInput) {
    authId
    username
    emails
    phoneNumbers { countryCode phoneNumber }
    isActive
  }
}
```

`getAccountInput`: `GetAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String!` | ใช่ |  |
| countryCode | `String` |  |  |

Response: `Account`

---

#### getAccountByUsername

ค้นบัญชีตาม username

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetAccountByUsername($getAccountByUsernameInput: GetAccountByUsernameInput) {
  getAccountByUsername(getAccountByUsernameInput: $getAccountByUsernameInput) {
    authId
    username
    isActive
  }
}
```

`getAccountByUsernameInput`: `GetAccountByUsernameInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String!` | ใช่ |  |

Response: `AccountUsername`

---

#### getAccountByPhoneNumber

ค้นบัญชีตามเบอร์โทรศัพท์

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetAccountByPhoneNumber($getAccountByPhoneNumberInput: GetAccountByPhoneNumberInput) {
  getAccountByPhoneNumber(getAccountByPhoneNumberInput: $getAccountByPhoneNumberInput) {
    authId
    username
    countryCode
    phoneNumber
    verifyStatus
    isActive
  }
}
```

`getAccountByPhoneNumberInput`: `GetAccountByPhoneNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |

Response: `AccountPhoneNumber`

---

#### getAccountByEmail

ค้นบัญชีตาม email

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetAccountByEmail($getAccountByEmailInput: GetAccountByEmailInput) {
  getAccountByEmail(getAccountByEmailInput: $getAccountByEmailInput) {
    authId
    username
    email
    verifyStatus
    isActive
  }
}
```

`getAccountByEmailInput`: `GetAccountByEmailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| email | `String!` | ใช่ |  |

Response: `AccountEmail`

---

#### getEmailsByAuthId

ดึง email ทั้งหมดของบัญชีตาม authId

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetEmailsByAuthId($authId: String!, $getInput: GetEmailsByAccountInput) {
  getEmailsByAuthId(authId: $authId, getInput: $getInput) {
    emails { authId username email verifyStatus isActive }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetEmailsByAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetEmailsByAccountFilterInput` |  |  |
| search | `GetEmailsByAccountSearchInput` |  |  |
| sort | `GetEmailsByAccountSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `authId: String!`

Response: `EmailAccountPagination`

---

#### getPhoneNumbersByAuthId

ดึงเบอร์โทรศัพท์ทั้งหมดของบัญชีตาม authId

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetPhoneNumbersByAuthId($authId: String!, $getInput: GetPhoneNumbersByAccountInput) {
  getPhoneNumbersByAuthId(authId: $authId, getInput: $getInput) {
    phoneNumbers { authId username countryCode phoneNumber verifyStatus isActive }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetPhoneNumbersByAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetPhoneNumbersByAccountFilterInput` |  |  |
| search | `GetPhoneNumbersByAccountSearchInput` |  |  |
| sort | `GetPhoneNumbersByAccountSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `authId: String!`

Response: `PhoneNumberAccountPagination`

---

#### getAccountsByAppKey

ดึงบัญชีทั้งหมดของแอปตาม appKey แบบแบ่งหน้า

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
query GetAccountsByAppKey($appKey: String!, $getInput: GetAccountByAppKeyInput) {
  getAccountsByAppKey(appKey: $appKey, getInput: $getInput) {
    accounts { authId username emails isActive }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetAccountByAppKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAccountByAppKeyFilterInput` |  |  |
| search | `GetAccountByAppKeySearchInput` |  |  |
| sort | `GetAccountByAppKeySortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String!`

Response: `AccountPagination`

---

#### getLockAccountsByAppKey

ดึงบัญชีที่ถูกล็อก (login ผิดเกินกำหนด) ของแอปตาม appKey แบบแบ่งหน้า

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getLockAccountsByAppKey`

```graphql
query GetLockAccountsByAppKey($appKey: String!, $getInput: GetAccountByAppKeyInput) {
  getLockAccountsByAppKey(appKey: $appKey, getInput: $getInput) {
    locks { authId username startDate endDate }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`getInput`: `GetAccountByAppKeyInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetAccountByAppKeyFilterInput` |  |  |
| search | `GetAccountByAppKeySearchInput` |  |  |
| sort | `GetAccountByAppKeySortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

argument อื่น: `appKey: String!`

Response: `AccountLockPagination`

---

#### getSessionLoggings

ดึงข้อมูล session logging แบบมีเงื่อนไข

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getSessionLoggings`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetSessionLoggings($input: GetSessionLoggingInput) {
  getSessionLoggings(input: $input) {
    sessionLoggings { _id appKey clientId totalSession startTime endTime }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetSessionLoggingInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| filter | `GetSessionLoggingFilterInput` |  |  |
| search | `GetSessionLoggingSearchInput` |  |  |
| sort | `GetSessionLoggingSortInput` |  |  |
| pagination | `CustomPaginateInput` |  |  |

Response: `SessionLoggingPagination!`

---

#### getSessionLoggingById

ดึงข้อมูล session logging โดยใช้ sessionLoggingId

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getSessionLoggingById`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetSessionLoggingById($sessionLoggingId: ID!) {
  getSessionLoggingById(sessionLoggingId: $sessionLoggingId) {
    _id
    appKey
    clientId
    totalSession
    startTime
    endTime
    createdAt
    updatedAt
  }
}
```

| argument | Type |
| --- | --- |
| sessionLoggingId | `ID!` |

Response: `SessionLogging`

---

#### getMyMobileDevices

ดึงอุปกรณ์ทั้งหมดของตัวเอง (ผู้ที่ login) แบบแบ่งหน้า — ไว้ให้ user มอนิเตอร์เครื่องของตัวเอง

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
query GetMyMobileDevices($input: GetMyMobileDevicesInput) {
  getMyMobileDevices(input: $input) {
    userMobileDevices { _id appKey installationId userId platform deviceName }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetMyMobileDevicesInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| status | `EnumDeviceStatus` |  | กรองตามสถานะเครื่อง (ถ้าไม่ระบุ = ทั้งหมด) |
| pagination | `CustomPaginateInput` |  | แบ่งหน้า |

Response: `UserMobileDevicePagination!`

---

#### getMobileDevicesByUser

ดึงอุปกรณ์ของผู้ใช้ที่ระบุ แบบแบ่งหน้า (ภายใน app เดียวกับผู้เรียก) — ต้องมีสิทธิ์ getMobileDevicesByUser

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getMobileDevicesByUser`

```graphql
query GetMobileDevicesByUser($input: GetMobileDevicesByUserInput!) {
  getMobileDevicesByUser(input: $input) {
    userMobileDevices { _id appKey installationId userId platform deviceName }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetMobileDevicesByUserInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| userId | `ID!` | ใช่ | authId ของผู้ใช้ที่จะดึงอุปกรณ์ |
| status | `EnumDeviceStatus` |  | กรองตามสถานะเครื่อง (ถ้าไม่ระบุ = ทั้งหมด) |
| pagination | `CustomPaginateInput` |  | แบ่งหน้า |

Response: `UserMobileDevicePagination!`

---

#### getMobileDevicesByApp

ดึงอุปกรณ์ทั้งหมดของ app ที่ระบุ แบบแบ่งหน้า — ต้องมีสิทธิ์ getMobileDevicesByApp (ข้าม app ต้องมี systemApp)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getMobileDevicesByApp`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
query GetMobileDevicesByApp($input: GetMobileDevicesByAppInput!) {
  getMobileDevicesByApp(input: $input) {
    userMobileDevices { _id appKey installationId userId platform deviceName }
    pagination { limit page totalItems totalPages hasPrevPage hasNextPage }
  }
}
```

`input`: `GetMobileDevicesByAppInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String!` | ใช่ | appKey ของ app ที่จะดึงอุปกรณ์ (ข้าม app ที่ login ต้องมีสิทธิ์ systemApp) |
| status | `EnumDeviceStatus` |  | กรองตามสถานะเครื่อง (ถ้าไม่ระบุ = ทั้งหมด) |
| pagination | `CustomPaginateInput` |  | แบ่งหน้า |

Response: `UserMobileDevicePagination!`

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

---

#### registerWithUsername

สมัครบัญชีด้วย username + password

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RegisterWithUsername($registerUsernameInput: RegisterUsernameInput) {
  registerWithUsername(registerUsernameInput: $registerUsernameInput) {
    status
  }
}
```

`registerUsernameInput`: `RegisterUsernameInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| confirmPassword | `String!` | ใช่ |  |
| roleKey | `RoleKeyEnum` |  |  |
| inviteCodeKey | `String` |  |  |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |
| firstName | `String` |  | firstName: ชื่อ |
| middleName | `String` |  | middleName: ชื่อกลาง |
| lastName | `String` |  | lastName: ชื่อสกุล |
| displayName | `String` |  | displayName: ชื่อที่ใช้แสดงผล |
| gender | `String` |  | gender: เพศ |
| profileImage | `String` |  | profileImage: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |
| electronicSignatureKey | `String` |  | electronicSignatureKey: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |

Response: `RegisterStatus`

---

#### registerWithEmail

สมัครบัญชีด้วย email + password

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RegisterWithEmail($registerEmailInput: RegisterEmailInput) {
  registerWithEmail(registerEmailInput: $registerEmailInput) {
    status
  }
}
```

`registerEmailInput`: `RegisterEmailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| email | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| confirmPassword | `String!` | ใช่ |  |
| roleKey | `RoleKeyEnum` |  |  |
| inviteCodeKey | `String` |  |  |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |
| firstName | `String` |  | firstName: ชื่อ |
| middleName | `String` |  | middleName: ชื่อกลาง |
| lastName | `String` |  | lastName: ชื่อสกุล |
| displayName | `String` |  | displayName: ชื่อที่ใช้แสดงผล |
| gender | `String` |  | gender: เพศ |
| profileImage | `String` |  | profileImage: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |
| electronicSignatureKey | `String` |  | electronicSignatureKey: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |

Response: `RegisterStatus`

---

#### registerWithPhoneNumber

สมัครบัญชีด้วยเบอร์โทรศัพท์ + password

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RegisterWithPhoneNumber($registerPhoneNumberInput: RegisterPhoneNumberInput) {
  registerWithPhoneNumber(registerPhoneNumberInput: $registerPhoneNumberInput) {
    status
  }
}
```

`registerPhoneNumberInput`: `RegisterPhoneNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| confirmPassword | `String!` | ใช่ |  |
| roleKey | `RoleKeyEnum` |  |  |
| inviteCodeKey | `String` |  |  |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |
| firstName | `String` |  | firstName: ชื่อ |
| middleName | `String` |  | middleName: ชื่อกลาง |
| lastName | `String` |  | lastName: ชื่อสกุล |
| displayName | `String` |  | displayName: ชื่อที่ใช้แสดงผล |
| gender | `String` |  | gender: เพศ |
| profileImage | `String` |  | profileImage: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |
| electronicSignatureKey | `String` |  | electronicSignatureKey: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |

Response: `RegisterStatus`

---

#### register

สมัครบัญชีด้วย username, email หรือเบอร์โทรศัพท์ (รวมทุกแบบไว้ใน mutation เดียว)

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation Register($registerInput: RegisterInput) {
  register(registerInput: $registerInput) {
    status
  }
}
```

`registerInput`: `RegisterInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String` |  |  |
| email | `String` |  |  |
| countryCode | `String` |  |  |
| phoneNumber | `String` |  |  |
| password | `String!` | ใช่ |  |
| confirmPassword | `String!` | ใช่ |  |
| roleKey | `RoleKeyEnum` |  |  |
| inviteCodeKey | `String` |  |  |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |
| firstName | `String` |  | firstName: ชื่อ |
| middleName | `String` |  | middleName: ชื่อกลาง |
| lastName | `String` |  | lastName: ชื่อสกุล |
| displayName | `String` |  | displayName: ชื่อที่ใช้แสดงผล |
| gender | `String` |  | gender: เพศ |
| profileImage | `String` |  | profileImage: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |
| electronicSignatureKey | `String` |  | electronicSignatureKey: fileKey ที่ได้จากการอัปโหลดไฟล์ไปยัง File Service |

Response: `RegisterStatus`

---

#### setAccountDefaultOrganization

กำหนด DefaultOrganization ของ Account ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation SetAccountDefaultOrganization($input: [SetAccountDefaultOrganizationInput]!) {
  setAccountDefaultOrganization(input: $input) {
    status
  }
}
```

`input`: `SetAccountDefaultOrganizationInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String!` | ใช่ |  |
| organizationKey | `String` |  |  |

Response: `RegisterStatus`

---

#### useInviteCode

ใช้ InviteCodeKey ในการลงทะเบียน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UseInviteCode($input: UseInviteCodeInput!) {
  useInviteCode(input: $input) {
    status
  }
}
```

`input`: `UseInviteCodeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| inviteCodeKey | `String!` | ใช่ |  |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `RegisterStatus`

---

#### addEmailToAccount

เพิ่ม email ให้บัญชี

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddEmailToAccount($input: AddEmailToAccountInput!) {
  addEmailToAccount(input: $input) {
    status
  }
}
```

`input`: `AddEmailToAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| email | `String!` | ใช่ |  |

Response: `RegisterStatus`

---

#### addPhoneNumberToAccount

เพิ่มเบอร์โทรศัพท์ให้บัญชี

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation AddPhoneNumberToAccount($input: AddPhoneNumberToAccountInput!) {
  addPhoneNumberToAccount(input: $input) {
    status
  }
}
```

`input`: `AddPhoneNumberToAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |

Response: `RegisterStatus`

---

#### updateAccount

อัปเดตข้อมูล Account ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateAccount`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateAccount($input: UpdateAccountInput!) {
  updateAccount(input: $input) {
    status
  }
}
```

`input`: `UpdateAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| isActive | `Boolean` |  |  |

Response: `RegisterStatus`

---

#### updateEmail

แก้ไข email ของบัญชี

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateEmail`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdateEmail($input: UpdateEmailInput!) {
  updateEmail(input: $input) {
    status
  }
}
```

`input`: `UpdateEmailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| email | `String!` | ใช่ |  |
| isActive | `Boolean` |  |  |

Response: `RegisterStatus`

---

#### updatePhoneNumber

อัปเดต phone number ของ Account ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updatePhoneNumber`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation UpdatePhoneNumber($input: UpdatePhoneNumberInput!) {
  updatePhoneNumber(input: $input) {
    status
  }
}
```

`input`: `UpdatePhoneNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |
| isActive | `Boolean` |  |  |

Response: `RegisterStatus`

---

#### removeEmailFromAccount

ลบ email ที่ระบุออกจาก Account ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeEmailFromAccount`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemoveEmailFromAccount($input: RemoveEmailInput!) {
  removeEmailFromAccount(input: $input) {
    status
  }
}
```

`input`: `RemoveEmailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| email | `String!` | ใช่ |  |

Response: `RegisterStatus`

---

#### removePhoneNumberFromAccount

ลบ phone number ที่ระบุออกจาก Account ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removePhoneNumberFromAccount`
- ทำงานข้ามแอปได้เมื่อมีสิทธิ์ `systemApp`

```graphql
mutation RemovePhoneNumberFromAccount($input: RemovePhoneNumberInput!) {
  removePhoneNumberFromAccount(input: $input) {
    status
  }
}
```

`input`: `RemovePhoneNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  |  |
| authId | `String!` | ใช่ |  |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |

Response: `RegisterStatus`

---

#### generateAppCredential

สร้าง AppCredential ใหม่ขึ้นมาในระบบ โดยที่ถ้า AppCredentialType = SYSTEM จะทำการสร้าง clientAuthId สำหรับ AppCredential นั้นด้วย

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `generateAppCredential`

```graphql
mutation GenerateAppCredential($input: GenerateAppCredentialInput!) {
  generateAppCredential(input: $input) {
    appCredential { _id appKey clientId description validateAuthorizedHosts validateAuthorizedRedirectUrls }
    clientSecret
  }
}
```

`input`: `GenerateAppCredentialInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| appKey | `String` |  | appKey ไว้ใช้ในการบอกว่าอยู่ app ใด ถ้าไม่ระบุจะใช้ app ที่ login อยู่ |
| clientId | `String` |  | clientId ไว้ใช้ในการใส่ไว้ใน headers เพื่อใช้งาน 'X-APP-CLIENT-ID' ถ้าไม่ระบุ ระบบ จะ auto gen ให้ |
| description | `String` |  | คำอธิบาย |
| authorizedHosts | `[AppCredentialAuthorizedHostInput]` |  | ตั้งค่าว่า AppCredential นี้สามารถใช้งานผ่าน hosts ใดได้บ้าง |
| validateAuthorizedHosts | `Boolean` |  | ตั้งค่าว่า AppCredential นี้จะตรวจสอบการใช้งานผ่าน hosts |
| authorizedRedirectUrls | `[AppCredentialAuthorizedRedirectUrlInput]` |  | ตั้งค่าว่า AppCredential นี้ยอมรับการ RedirectUrl ใดบ้าง |
| validateAuthorizedRedirectUrls | `Boolean` |  | ตั้งค่าว่า AppCredential นี้จะตรวจสอบการ RedirectUrl |
| minutesTimeLock | `Int` |  | จำนวนนาที ที่ lock เมื่อ login ผิด |
| numberOfFail | `Int` |  | จำนวนครั้งที่ login ผิด แล้วจะ lock |
| minutesTimeFail | `Int` |  | จำนวนนาที ที่เมื่อ login ผิด แล้วจะไม่ให้ login ซ้ำ ก่อนจะ lock |
| jwtAccessExpireTime | `Int` |  | เวลาหมดอายุของ Access token (วินาที) ถ้าเป็น 0 คือไม่มีวันหมดอายุ |
| jwtRefreshExpireTime | `Int` |  | เวลาหมดอายุของ Refresh token (วินาที) ถ้าเป็น 0 คือไม่มีวันหมดอายุ |
| expiryAt | `Date` |  | เวลาหมดอายุของ AppCredential นี้(ถ้ามี) |
| appCredentialType | `EnumAppCredentialType` |  | ประเภทของ appCredential |
| minimumAppVersion | `String` |  | เวอร์ชันขั้นต่ำของ mobile app ที่อนุญาตให้ใช้งาน |
| appUpdateUrlAndroid | `String` |  | ลิงก์อัปเดตแอปสำหรับ Android |
| appUpdateUrlIos | `String` |  | ลิงก์อัปเดตแอปสำหรับ iOS |
| … | | | (มีอีก 4 ฟิลด์ ดู schema) |

Response: `GenerateAppCredential!`

---

#### updateAppCredential

แก้ไขข้อมูล AppCredential โดยจะแก้ได้แค่ข้อมูลการตั้งค่าบางส่วนเท่านั้น

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `updateAppCredential`

```graphql
mutation UpdateAppCredential($appCredentialId: ID!, $input: UpdateAppCredentialInput!) {
  updateAppCredential(appCredentialId: $appCredentialId, input: $input) {
    _id
    appKey
    clientId
    description
    authorizedHosts { _id hostName description isActive }
    validateAuthorizedHosts
    authorizedRedirectUrls { _id redirectUrl description isActive }
    validateAuthorizedRedirectUrls
    minutesTimeLock
    numberOfFail
    # ...
  }
}
```

`input`: `UpdateAppCredentialInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| description | `String` |  | คำอธิบาย |
| authorizedHosts | `[AppCredentialAuthorizedHostInput]` |  | ตั้งค่าว่า AppCredential นี้สามารถใช้งานผ่าน hosts ใดได้บ้าง |
| validateAuthorizedHosts | `Boolean` |  | ตั้งค่าว่า AppCredential นี้จะตรวจสอบการใช้งานผ่าน hosts |
| authorizedRedirectUrls | `[AppCredentialAuthorizedRedirectUrlInput]` |  | ตั้งค่าว่า AppCredential นี้ยอมรับการ RedirectUrl ใดบ้าง |
| validateAuthorizedRedirectUrls | `Boolean` |  | ตั้งค่าว่า AppCredential นี้จะตรวจสอบการ RedirectUrl |
| minutesTimeLock | `Int` |  | จำนวนนาที ที่ lock เมื่อ login ผิด |
| numberOfFail | `Int` |  | จำนวนครั้งที่ login ผิด แล้วจะ lock |
| minutesTimeFail | `Int` |  | จำนวนนาที ที่เมื่อ login ผิด แล้วจะไม่ให้ login ซ้ำ ก่อนจะ lock |
| jwtAccessExpireTime | `Int` |  | เวลาหมดอายุของ Access token (วินาที) ถ้าเป็น 0 คือไม่มีวันหมดอายุ |
| jwtRefreshExpireTime | `Int` |  | เวลาหมดอายุของ Refresh token (วินาที) ถ้าเป็น 0 คือไม่มีวันหมดอายุ |
| expiryAt | `Date` |  | เวลาหมดอายุของ AppCredential นี้(ถ้ามี) |
| minimumAppVersion | `String` |  | เวอร์ชันขั้นต่ำของ mobile app ที่อนุญาตให้ใช้งาน |
| appUpdateUrlAndroid | `String` |  | ลิงก์อัปเดตแอปสำหรับ Android |
| appUpdateUrlIos | `String` |  | ลิงก์อัปเดตแอปสำหรับ iOS |
| forceUpdate | `Boolean` |  | บังคับอัปเดตเมื่อเวอร์ชันต่ำกว่า minimumAppVersion |
| isActive | `Boolean` |  | สถานะการใช้งาน |

argument อื่น: `appCredentialId: ID!`

Response: `AppCredential!`

---

#### removeAppCredential

ลบ AppCredential นั้นออกจากระบบ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `removeAppCredential`

```graphql
mutation RemoveAppCredential($appCredentialId: ID!) {
  removeAppCredential(appCredentialId: $appCredentialId) {
    _id
    appKey
    clientId
    description
    authorizedHosts { _id hostName description isActive }
    validateAuthorizedHosts
    authorizedRedirectUrls { _id redirectUrl description isActive }
    validateAuthorizedRedirectUrls
    minutesTimeLock
    numberOfFail
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| appCredentialId | `ID!` |

Response: `AppCredential!`

---

#### loginWithAccessType

login ด้วย username, email หรือเบอร์โทรศัพท์ + password แล้วได้ token กลับทันที · ถ้าใช้เบอร์โทรศัพท์ ควรส่ง `countryCode` (เช่น `+66`) หรือใส่เบอร์ในรูป `+66...`

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithAccessType($loginWithAccessTypeInput: LoginWithAccessTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithAccessType(loginWithAccessTypeInput: $loginWithAccessTypeInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithAccessTypeInput`: `LoginWithAccessTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String!` | ใช่ |  |
| countryCode | `String` |  |  |
| password | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### loginWithGoogleOAuth

login ด้วย Google (ส่ง `id_token` จาก Google)

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithGoogleOAuth($loginWithGoogleOAuthInput: LoginWithGoogleOAuthInput, $sessionInfo: SessionInfoInput) {
  loginWithGoogleOAuth(loginWithGoogleOAuthInput: $loginWithGoogleOAuthInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithGoogleOAuthInput`: `LoginWithGoogleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### registerWithGoogleOAuth

สมัครบัญชีใหม่ด้วย Google (`id_token`) แล้วได้ token กลับ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RegisterWithGoogleOAuth($registerWithGoogleOAuthInput: RegisterWithGoogleOAuthInput, $sessionInfo: SessionInfoInput) {
  registerWithGoogleOAuth(registerWithGoogleOAuthInput: $registerWithGoogleOAuthInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`registerWithGoogleOAuthInput`: `RegisterWithGoogleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### linkAccountWithGoogleOAuth

ผูกบัญชี Google เข้ากับบัญชีที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation LinkAccountWithGoogleOAuth($linkAccountWithGoogleOAuthInput: LinkAccountWithGoogleOAuthInput) {
  linkAccountWithGoogleOAuth(linkAccountWithGoogleOAuthInput: $linkAccountWithGoogleOAuthInput) {
    status
  }
}
```

`linkAccountWithGoogleOAuthInput`: `LinkAccountWithGoogleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |

Response: `LinkAccountWithGoogleOAuthStatus`

---

#### unlinkAccountFromGoogleOAuth

ยกเลิกการผูกบัญชี Google ออกจากบัญชีที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UnlinkAccountFromGoogleOAuth($unlinkAccountFromGoogleOAuthInput: UnlinkAccountFromGoogleOAuthInput) {
  unlinkAccountFromGoogleOAuth(unlinkAccountFromGoogleOAuthInput: $unlinkAccountFromGoogleOAuthInput) {
    status
  }
}
```

`unlinkAccountFromGoogleOAuthInput`: `UnlinkAccountFromGoogleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String` |  |  |

Response: `UnlinkAccountFromGoogleOAuthStatus`

---

#### loginWithFaceBookOAuth

login ด้วย Facebook (ส่ง `access_token` จาก Facebook)

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithFaceBookOAuth($loginWithFaceBookOAuthInput: LoginWithFaceBookOAuthInput, $sessionInfo: SessionInfoInput) {
  loginWithFaceBookOAuth(loginWithFaceBookOAuthInput: $loginWithFaceBookOAuthInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithFaceBookOAuthInput`: `LoginWithFaceBookOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### registerWithFaceBookOAuth

สมัครบัญชีใหม่ด้วย Facebook (`access_token`) แล้วได้ token กลับ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RegisterWithFaceBookOAuth($registerWithFaceBookOAuthInput: RegisterWithFaceBookOAuthInput, $sessionInfo: SessionInfoInput) {
  registerWithFaceBookOAuth(registerWithFaceBookOAuthInput: $registerWithFaceBookOAuthInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`registerWithFaceBookOAuthInput`: `RegisterWithFaceBookOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### linkAccountWithFaceBookOAuth

ผูกบัญชี Facebook เข้ากับบัญชีที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation LinkAccountWithFaceBookOAuth($linkAccountWithFaceBookOAuthInput: LinkAccountWithFaceBookOAuthInput) {
  linkAccountWithFaceBookOAuth(linkAccountWithFaceBookOAuthInput: $linkAccountWithFaceBookOAuthInput) {
    status
  }
}
```

`linkAccountWithFaceBookOAuthInput`: `LinkAccountWithFaceBookOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |

Response: `LinkAccountWithFaceBookOAuthStatus`

---

#### unlinkAccountFromFaceBookOAuth

ยกเลิกการผูกบัญชี Facebook ออกจากบัญชีที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UnlinkAccountFromFaceBookOAuth($unlinkAccountFromFaceBookOAuthInput: UnlinkAccountFromFaceBookOAuthInput) {
  unlinkAccountFromFaceBookOAuth(unlinkAccountFromFaceBookOAuthInput: $unlinkAccountFromFaceBookOAuthInput) {
    status
  }
}
```

`unlinkAccountFromFaceBookOAuthInput`: `UnlinkAccountFromFaceBookOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String` |  |  |

Response: `UnlinkAccountFromFaceBookOAuthStatus`

---

#### loginWithAppleOAuth

login ด้วย Apple (ส่ง `id_token` จาก Apple)

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithAppleOAuth($loginWithAppleOAuthInput: LoginWithAppleOAuthInput, $sessionInfo: SessionInfoInput) {
  loginWithAppleOAuth(loginWithAppleOAuthInput: $loginWithAppleOAuthInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithAppleOAuthInput`: `LoginWithAppleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### registerWithAppleOAuth

สมัครบัญชีใหม่ด้วย Apple (`id_token`) แล้วได้ token กลับ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RegisterWithAppleOAuth($registerWithAppleOAuthInput: RegisterWithAppleOAuthInput, $sessionInfo: SessionInfoInput) {
  registerWithAppleOAuth(registerWithAppleOAuthInput: $registerWithAppleOAuthInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`registerWithAppleOAuthInput`: `RegisterWithAppleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### linkAccountWithAppleOAuth

ผูกบัญชี Apple เข้ากับบัญชีที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation LinkAccountWithAppleOAuth($linkAccountWithAppleOAuthInput: LinkAccountWithAppleOAuthInput) {
  linkAccountWithAppleOAuth(linkAccountWithAppleOAuthInput: $linkAccountWithAppleOAuthInput) {
    status
  }
}
```

`linkAccountWithAppleOAuthInput`: `LinkAccountWithAppleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| idToken | `String!` | ใช่ |  |

Response: `LinkAccountWithAppleOAuthStatus`

---

#### unlinkAccountFromAppleOAuth

ยกเลิกการผูกบัญชี Apple ออกจากบัญชีที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UnlinkAccountFromAppleOAuth($unlinkAccountFromAppleOAuthInput: UnlinkAccountFromAppleOAuthInput) {
  unlinkAccountFromAppleOAuth(unlinkAccountFromAppleOAuthInput: $unlinkAccountFromAppleOAuthInput) {
    status
  }
}
```

`unlinkAccountFromAppleOAuthInput`: `UnlinkAccountFromAppleOAuthInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String` |  |  |

Response: `UnlinkAccountFromAppleOAuthStatus`

---

#### loginWithUserNameAccessType

login ด้วย username + password แล้วได้ token กลับทันที

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithUserNameAccessType($loginWithUsernameAccessTypeInput: LoginWithUsernameAccessTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithUserNameAccessType(loginWithUsernameAccessTypeInput: $loginWithUsernameAccessTypeInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithUsernameAccessTypeInput`: `LoginWithUsernameAccessTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### loginWithEmailAccessType

login ด้วย email + password แล้วได้ token กลับทันที

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithEmailAccessType($loginWithEmailAccessTypeInput: LoginWithEmailAccessTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithEmailAccessType(loginWithEmailAccessTypeInput: $loginWithEmailAccessTypeInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithEmailAccessTypeInput`: `LoginWithEmailAccessTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| email | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### loginWithPhoneNumberAccessType

login ด้วยเบอร์โทรศัพท์ + password แล้วได้ token กลับทันที

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithPhoneNumberAccessType($loginWithPhoneNumberAccessTypeInput: LoginWithPhoneNumberAccessTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithPhoneNumberAccessType(loginWithPhoneNumberAccessTypeInput: $loginWithPhoneNumberAccessTypeInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
  }
}
```

`loginWithPhoneNumberAccessTypeInput`: `LoginWithPhoneNumberAccessTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| redirectURL | `String` |  |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginAccessType`

---

#### loginWithUserNameSocketType

login ด้วย username + password แบบ WebSocket: token จะถูกส่งเข้า socket ที่ได้จาก `getLoginSocketId`

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithUserNameSocketType($loginWithUsernameSocketTypeInput: LoginWithUsernameSocketTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithUserNameSocketType(loginWithUsernameSocketTypeInput: $loginWithUsernameSocketTypeInput, sessionInfo: $sessionInfo) {
    status
  }
}
```

`loginWithUsernameSocketTypeInput`: `LoginWithUsernameSocketTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| username | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| socketId | `String!` | ใช่ |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginSocketType!`

---

#### loginWithEmailSocketType

login ด้วย email + password แบบ WebSocket

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithEmailSocketType($loginWithEmailSocketTypeInput: LoginWithEmailSocketTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithEmailSocketType(loginWithEmailSocketTypeInput: $loginWithEmailSocketTypeInput, sessionInfo: $sessionInfo) {
    status
  }
}
```

`loginWithEmailSocketTypeInput`: `LoginWithEmailSocketTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| email | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| socketId | `String!` | ใช่ |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginSocketType!`

---

#### loginWithPhoneNumberSocketType

login ด้วยเบอร์โทรศัพท์ + password แบบ WebSocket

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithPhoneNumberSocketType($loginWithPhoneNumberSocketType: LoginWithPhoneNumberSocketTypeInput, $sessionInfo: SessionInfoInput) {
  loginWithPhoneNumberSocketType(loginWithPhoneNumberSocketType: $loginWithPhoneNumberSocketType, sessionInfo: $sessionInfo) {
    status
  }
}
```

`loginWithPhoneNumberSocketType`: `LoginWithPhoneNumberSocketTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |
| password | `String!` | ใช่ |  |
| socketId | `String!` | ใช่ |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `LoginSocketType!`

---

#### loginWithEmailOtpType

ขอ OTP ทาง email เพื่อ login · ได้ `otpRef` กลับ แล้วนำไปยืนยันด้วย `verifierOtp`

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithEmailOtpType($loginWithEmailOtpTypeInput: LoginWithEmailOtpTypeInput) {
  loginWithEmailOtpType(loginWithEmailOtpTypeInput: $loginWithEmailOtpTypeInput) {
    redirectURL
    otpRef
  }
}
```

`loginWithEmailOtpTypeInput`: `LoginWithEmailOtpTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| email | `String!` | ใช่ |  |
| socketId | `String!` | ใช่ |  |

Response: `LoginOtpType!`

---

#### loginWithPhoneNumberOtpType

ขอ OTP ทาง SMS เพื่อ login · ได้ `otpRef` กลับ แล้วนำไปยืนยันด้วย `verifierOtp`

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation LoginWithPhoneNumberOtpType($loginWithPhoneNumberOtpTypeInput: LoginWithPhoneNumberOtpTypeInput) {
  loginWithPhoneNumberOtpType(loginWithPhoneNumberOtpTypeInput: $loginWithPhoneNumberOtpTypeInput) {
    redirectURL
    otpRef
  }
}
```

`loginWithPhoneNumberOtpTypeInput`: `LoginWithPhoneNumberOtpTypeInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |
| socketId | `String!` | ใช่ |  |

Response: `LoginOtpType!`

---

#### logout

ออกจากระบบ session ปัจจุบัน

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation Logout {
  logout {
    status
  }
}
```

Response: `LogoutStatus`

---

#### logoutAll

ออกจากระบบทุก session ของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation LogoutAll {
  logoutAll {
    status
  }
}
```

Response: `LogoutStatus`

---

#### logoutAllByAuthId

ออกจากระบบทุก Session ตาม Auth ID

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `logoutAllByAuthId`

```graphql
mutation LogoutAllByAuthId($authId: String!) {
  logoutAllByAuthId(authId: $authId) {
    status
  }
}
```

| argument | Type |
| --- | --- |
| authId | `String!` |

Response: `LogoutStatus`

---

#### refreshToken

ขอ accessToken / refreshToken ชุดใหม่ด้วย refreshToken (refreshToken เดิมใช้ซ้ำไม่ได้)

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation RefreshToken($refreshTokenInput: RefreshTokenInput) {
  refreshToken(refreshTokenInput: $refreshTokenInput) {
    accessToken
    refreshToken
  }
}
```

`refreshTokenInput`: `RefreshTokenInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| refreshToken | `String!` | ใช่ |  |

Response: `Token`

---

#### changePassword

เปลี่ยนรหัสผ่านของผู้ใช้ที่ login อยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation ChangePassword($changePasswordInput: ChangePasswordInput) {
  changePassword(changePasswordInput: $changePasswordInput) {
    status
  }
}
```

`changePasswordInput`: `ChangePasswordInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| currentPassword | `String!` | ใช่ |  |
| newPassword | `String!` | ใช่ |  |
| confirmNewPassword | `String!` | ใช่ |  |

Response: `ChangePasswordStatus`

---

#### resetPassword

ตั้งรหัสผ่านใหม่ให้บัญชี (ผู้ดูแล)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation ResetPassword($resetPasswordInput: ResetPasswordInput) {
  resetPassword(resetPasswordInput: $resetPasswordInput) {
    status
  }
}
```

`resetPasswordInput`: `ResetPasswordInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String!` | ใช่ |  |
| newPassword | `String!` | ใช่ |  |
| confirmNewPassword | `String!` | ใช่ |  |

Response: `ResetPasswordStatus`

---

#### resetPasswordWithEmail

ขอ OTP ทาง email เพื่อรีเซ็ตรหัสผ่าน

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation ResetPasswordWithEmail($resetPasswordWithEmailInput: ResetPasswordWithEmailInput) {
  resetPasswordWithEmail(resetPasswordWithEmailInput: $resetPasswordWithEmailInput) {
    otpRef
  }
}
```

`resetPasswordWithEmailInput`: `ResetPasswordWithEmailInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| email | `String!` | ใช่ |  |

Response: `ResetPasswordOtp`

---

#### resetPasswordWithPhoneNumber

ขอ OTP ทาง SMS เพื่อรีเซ็ตรหัสผ่าน

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation ResetPasswordWithPhoneNumber($resetPasswordWithPhoneNumberInput: ResetPasswordWithPhoneNumberInput) {
  resetPasswordWithPhoneNumber(resetPasswordWithPhoneNumberInput: $resetPasswordWithPhoneNumberInput) {
    otpRef
  }
}
```

`resetPasswordWithPhoneNumberInput`: `ResetPasswordWithPhoneNumberInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| countryCode | `String!` | ใช่ |  |
| phoneNumber | `String!` | ใช่ |  |

Response: `ResetPasswordOtp`

---

#### revokeToken

ยกเลิก token ของผู้ใช้ที่ระบุ

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation RevokeToken($revokeTokenInput: RevokeTokenInput) {
  revokeToken(revokeTokenInput: $revokeTokenInput) {
    status
  }
}
```

`revokeTokenInput`: `RevokeTokenInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String!` | ใช่ |  |

Response: `RevokeTokenStatus`

---

#### unlockAccountByAuthId

ปลดล็อกบัญชีที่ถูกล็อกตาม authId

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM
- สิทธิ์: `getLockAccountsByAppKey`

```graphql
mutation UnlockAccountByAuthId($appKey: String!, $authId: String!) {
  unlockAccountByAuthId(appKey: $appKey, authId: $authId) {
    status
  }
}
```

| argument | Type |
| --- | --- |
| appKey | `String!` |
| authId | `String!` |

Response: `DeleteAccountStatus`

---

#### deleteAccount

ลบบัญชี

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation DeleteAccount($deleteAccountInput: DeleteAccountInput) {
  deleteAccount(deleteAccountInput: $deleteAccountInput) {
    status
  }
}
```

`deleteAccountInput`: `DeleteAccountInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| authId | `String!` | ใช่ |  |

Response: `DeleteAccountStatus`

---

#### verifierOtp

ยืนยัน OTP ที่ได้จาก `loginWith*OtpType` หรือ `resetPasswordWith*` · กรณี login ได้ token กลับ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation VerifierOtp($verifierOtpInput: VerifierOtpInput, $sessionInfo: SessionInfoInput) {
  verifierOtp(verifierOtpInput: $verifierOtpInput, sessionInfo: $sessionInfo) {
    redirectURL
    token { accessToken refreshToken }
    otpVerifierRef
    status
  }
}
```

`verifierOtpInput`: `VerifierOtpInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| otpRef | `String!` | ใช่ |  |
| otpCode | `String!` | ใช่ |  |

`sessionInfo`: `SessionInfoInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| host | `String` |  |  |
| ipAddress | `String` |  |  |
| userAgent | `String` |  |  |

Response: `VerifierOtp`

---

#### resetMyPasswordByOtpVerifierRef

ตั้งรหัสผ่านใหม่หลังยืนยัน OTP สำเร็จ

- การยืนยันตัวตน: ระดับแอป: header `X-APP-CLIENT-ID` (ไม่ต้อง login)

```graphql
mutation ResetMyPasswordByOtpVerifierRef($resetPasswordOtpInput: ResetPasswordOtpInput) {
  resetMyPasswordByOtpVerifierRef(resetPasswordOtpInput: $resetPasswordOtpInput) {
    status
  }
}
```

`resetPasswordOtpInput`: `ResetPasswordOtpInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| otpVerifierRef | `String!` | ใช่ |  |
| newPassword | `String!` | ใช่ |  |
| confirmNewPassword | `String!` | ใช่ |  |

Response: `ResetPasswordStatus`

---

#### syncMobileDevice

ลงทะเบียน/อัปเดตอุปกรณ์ของผู้ใช้ (เรียกหลัง login) — upsert ตาม installationId + user ที่ login

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation SyncMobileDevice($input: SyncMobileDeviceInput!) {
  syncMobileDevice(input: $input) {
    _id
    appKey
    installationId
    userId
    platform
    deviceName
    appVersion
    pushProvider
    pushEnabled
    pushPermissionStatus
    # ...
  }
}
```

`input`: `SyncMobileDeviceInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| installationId | `String!` | ใช่ | installationId ที่ mobile gen |
| platform | `EnumMobilePlatform!` | ใช่ | แพลตฟอร์มของเครื่อง |
| deviceName | `String` |  | ชื่อเครื่อง |
| appVersion | `String` |  | เวอร์ชันแอป |

Response: `UserMobileDevice!`

---

#### registerPushToken

ลงทะเบียน Expo push token ให้เครื่องนี้ (เรียกหลังได้ token จาก OS) — ย้าย token ออกจากเครื่องอื่นที่ถืออยู่

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation RegisterPushToken($input: RegisterPushTokenInput!) {
  registerPushToken(input: $input) {
    _id
    appKey
    installationId
    userId
    platform
    deviceName
    appVersion
    pushProvider
    pushEnabled
    pushPermissionStatus
    # ...
  }
}
```

`input`: `RegisterPushTokenInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| installationId | `String!` | ใช่ | installationId ที่ mobile gen |
| pushToken | `String!` | ใช่ | Expo push token |
| pushProvider | `EnumPushProvider` |  | ผู้ให้บริการ push |
| pushPermissionStatus | `EnumPushPermissionStatus` |  | สถานะสิทธิ์การแจ้งเตือนจาก OS |
| pushLanguage | `String` |  | ภาษาของการแจ้งเตือน |

Response: `UserMobileDevice!`

---

#### updateMobileDeviceSetting

อัปเดตการตั้งค่าการแจ้งเตือนของเครื่อง (เปิด/ปิด, ภาษา, ระดับความสำคัญ)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation UpdateMobileDeviceSetting($input: UpdateMobileDeviceSettingInput!) {
  updateMobileDeviceSetting(input: $input) {
    _id
    appKey
    installationId
    userId
    platform
    deviceName
    appVersion
    pushProvider
    pushEnabled
    pushPermissionStatus
    # ...
  }
}
```

`input`: `UpdateMobileDeviceSettingInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| installationId | `String!` | ใช่ | installationId ที่ mobile gen |
| pushEnabled | `Boolean` |  | เปิด/ปิดการแจ้งเตือน |
| pushLanguage | `String` |  | ภาษาของการแจ้งเตือน |
| minimumSeverity | `EnumPushSeverity` |  | ระดับความสำคัญขั้นต่ำที่จะรับการแจ้งเตือน |
| pushPermissionStatus | `EnumPushPermissionStatus` |  | สถานะสิทธิ์การแจ้งเตือนจาก OS (กรณีผู้ใช้เปลี่ยนใน setting ของเครื่อง) |

Response: `UserMobileDevice!`

---

#### deactivateMobileDevice

ปิดการใช้งานเครื่องนี้ (เรียกตอน logout) — set inactive + ปิด push (ไม่ลบ document)

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation DeactivateMobileDevice($input: DeactivateMobileDeviceInput!) {
  deactivateMobileDevice(input: $input) {
    _id
    appKey
    installationId
    userId
    platform
    deviceName
    appVersion
    pushProvider
    pushEnabled
    pushPermissionStatus
    # ...
  }
}
```

`input`: `DeactivateMobileDeviceInput`

| key | Type | จำเป็น | คำอธิบาย |
| --- | --- | --- | --- |
| installationId | `String!` | ใช่ | installationId ที่ mobile gen |

Response: `UserMobileDevice!`

---

#### removeMyMobileDevice

ลบอุปกรณ์ของตัวเองออกถาวร (hard delete) — สำหรับเครื่องที่ไม่ใช้แล้ว

- การยืนยันตัวตน: ต้อง login (header `authorization`) หรือใช้ credential แบบ SYSTEM

```graphql
mutation RemoveMyMobileDevice($installationId: String!) {
  removeMyMobileDevice(installationId: $installationId) {
    _id
    appKey
    installationId
    userId
    platform
    deviceName
    appVersion
    pushProvider
    pushEnabled
    pushPermissionStatus
    # ...
  }
}
```

| argument | Type |
| --- | --- |
| installationId | `String!` |

Response: `UserMobileDevice!`

---

<br>
<br>

## Kafka consume Reference

ทุกข้อความมี header `appKey` และ `serviceKey` (service ปลายทาง) · payload อยู่ในรูป `{ <ข้อมูล>: {...}, action: "ADD" | "REMOVE" | ... }` · ข้อความของแอปที่ service นี้ไม่มี AppCertificate จะถูกข้าม

---

### init-system

ตั้งระบบจากศูนย์ (เฉพาะ core set) — สร้างแอป SYSTEM, AppCredential เริ่มต้น และบัญชีผู้ดูแลระบบ

    topic: init-system

---

### refresh-data

core สั่งให้ส่งข้อมูลที่ตัวเองถืออยู่ขึ้น Kafka ใหม่ (ใช้ตอนมี service ใหม่เข้าแอป) และล้าง cache ของตัวเอง · รับเฉพาะข้อความที่ header `serviceKey` = `authentication`

    topic: refresh-data

เมื่อได้รับจะส่ง `sync-app-credential` ของแอปนั้นให้ทุก service และส่ง `sync-permission` ของตัวเองให้ ACL

---

### sync-app-certificate

รับ AppCertificate (กุญแจของ service ต่อแอป) จาก core — บอกว่า service ใดอยู่ในแอปใด

    topic: sync-app-certificate

---

### sync-service-setting / sync-app-service-setting

รับค่าตั้งค่าเพิ่มเติมแบบ JSON ของ service นี้ ทั้งระบบ (`sync-service-setting`) และรายแอป (`sync-app-service-setting`) จาก core · ค่า OAuth ของ Google / Facebook / Apple ตั้งผ่าน `sync-app-service-setting` (ดู [วิธี login ที่มี](#login-methods))

    topic: sync-service-setting
    topic: sync-app-service-setting

---

### sync-application

รับข้อมูลแอปจาก Application Service

    topic: sync-application

---

### sync-user-policy

รับ UserPolicy ที่ ACL คอมไพล์แล้ว เฉพาะ permission ของ `authentication` ใช้ตรวจสิทธิ์ตอนเรียก API

    topic: sync-user-policy

| key | Type | คำอธิบาย |
| --- | --- | --- |
| userPolicy.userPolicyKey | string | `authentication::<permissionKey>:<appKey>::<authId>` หรือ `authentication::<permissionKey>:<appKey>:<organizationId>:<authId>` |
| userPolicy.permissionKey | string | permission |
| userPolicy.authId | string | ผู้ใช้ |
| userPolicy.organizationId | string | องค์กร (ถ้าเป็นสิทธิ์ระดับองค์กร) |
| action | string | `ADD`, `REMOVE`, `REMOVE_APP`, `REMOVE_PERMISSION`, `REMOVE_USER`, `REMOVE_ORGANIZATION` |

---

### sync-organization

รับข้อมูลองค์กรจาก [Unit Service](unitService.md) (ใช้กับ default organization ของบัญชี)

    topic: sync-organization

---

### sync-invite-code

รับข้อมูล invite code จาก [ACL Service](aclService.md) ใช้ตอนสมัครด้วย `inviteCodeKey` หรือ `useInviteCode`

    topic: sync-invite-code

| key | Type | คำอธิบาย |
| --- | --- | --- |
| inviteCode.id | string | id |
| inviteCode.inviteCodeKey | string | รหัสเชิญ |
| inviteCode.defaultOrganizationId / defaultOrganizationKey | string | องค์กรเริ่มต้นที่จะตั้งให้ผู้ใช้ |
| inviteCode.maxUses / currentUses | number | จำนวนครั้งที่ใช้ได้ / ใช้ไปแล้ว |
| inviteCode.validStartTime / validEndTime | Date | ช่วงเวลาที่ใช้ได้ |
| inviteCode.appRoleIds / orgRoleIds | string[] | role ที่จะได้รับ |
| inviteCode.isActive | boolean | เปิดใช้งาน |
| action | string | `ADD`, `REMOVE` |

---

## Kafka Produce Reference

---

### sync-auth

ส่งข้อมูลบัญชีเมื่อมีการสมัคร (รวมสมัครผ่าน OAuth), ตั้ง default organization หรือลบบัญชี — **กระจายถึงทุก service ในแอป** service ไหนต้องใช้ชื่อ / อีเมล / เบอร์โทรของผู้ใช้ (เช่น ส่ง SMS) รับ topic นี้แล้วเก็บสำเนาไว้ได้เลย · ผู้รับปัจจุบัน: Profile, ACL

    topic: sync-auth
    header: appKey (ไม่มี serviceKey = กระจายทั้งแอป · ผู้รับห้ามกรองด้วย serviceKey)

| key | Type | คำอธิบาย |
| --- | --- | --- |
| account.appKey | string | appKey |
| account.authId | string | id ของบัญชี |
| account.username | string | username |
| account.emails / phoneNumber | array | email / เบอร์โทรของบัญชี |
| account.roleKey | string | `ADMIN`, `NONE` |
| account.inviteCodeId / inviteCodeKey | string | invite code ที่ใช้สมัคร (ถ้ามี) |
| account.userType | string | ประเภทผู้ใช้ |
| account.defaultOrganizationId / defaultOrganizationKey | string | องค์กรเริ่มต้น |
| account.firstName, middleName, lastName, displayName, gender, profileImage, electronicSignatureKey | string | ข้อมูลโปรไฟล์เริ่มต้น |
| action | string | `ADD`, `REMOVE` |

> ค่า message ของ topic นี้ถูกห่อเป็น `{ "value": "<JSON string>" }` ผู้รับต้อง `JSON.parse` ค่า `value` อีกชั้นเพื่อได้ object ข้างบน

---

### sync-app-credential

กระจาย AppCredential ของแอปไปให้ทุก service ที่อยู่ในแอปนั้น (ส่งแยกทีละ service, header `serviceKey` = service ปลายทาง) เพื่อให้แต่ละ service ตรวจ token และ header ของผู้เรียกได้เอง · ข้อมูลลับในข้อความถูกเข้ารหัสด้วยกุญแจของ service ปลายทาง

    topic: sync-app-credential

| key | Type | คำอธิบาย |
| --- | --- | --- |
| appCredential.clientId | string | clientId |
| appCredential.appCredentialType | string | `USER`, `SYSTEM` |
| appCredential.authorizedHosts / authorizedRedirectUrls | array | host / redirect URL ที่อนุญาต |
| appCredential.jwtAccessExpireTime / jwtRefreshExpireTime | number | อายุ token (วินาที) |
| appCredential.numberOfFail / minutesTimeFail / minutesTimeLock | number | กฎการล็อกบัญชี |
| appCredential.expiryAt | Date | วันหมดอายุของ AppCredential |
| appCredential.minimumAppVersion, appUpdateUrl*, forceUpdate | | การบังคับอัปเดตแอปมือถือ |
| appCredential.(กุญแจตรวจ token / clientSecret) | string | เข้ารหัสถึง service ปลายทาง |
| action | string | `ADD`, `REMOVE` |

---

### create-notification

ส่งคำขอแจ้งเตือนไปที่ Notification Service เช่น OTP ทาง SMS หรือ email

    topic: create-notification

| key | Type | คำอธิบาย |
| --- | --- | --- |
| notification.isSchedule | boolean | `false` = ส่งทันที |
| notification.sms | object | `msisdn`, `message`, `sender`, `expire` |
| notification.email | object | `to`, `subject`, `text` |

---

### sync-mobile-device

ส่งข้อมูลอุปกรณ์มือถือ / push token ที่เปลี่ยนแปลงไปให้ Notification Service

    topic: sync-mobile-device

| key | Type | คำอธิบาย |
| --- | --- | --- |
| mobileDevice.appKey | string | appKey |
| mobileDevice.installationId | string | id ของการติดตั้งแอปบนเครื่อง |
| mobileDevice.userId | string | ผู้ใช้ |
| mobileDevice.platform | string | แพลตฟอร์ม |
| mobileDevice.deviceName | string | ชื่อเครื่อง |
| mobileDevice.appVersion | string | เวอร์ชันแอป |
| mobileDevice.pushToken / pushProvider / pushEnabled / pushPermissionStatus | | push token และสถานะการแจ้งเตือน |
| mobileDevice.pushLanguage / minimumSeverity | string | ภาษา / ระดับความสำคัญขั้นต่ำที่ต้องการรับ |
| mobileDevice.status | string | สถานะเครื่อง |
| action | string | `ADD`, `REMOVE` |

---

### sync-permission

ส่ง permission ทั้งหมดของ `authentication` ให้ [ACL Service](aclService.md) (ส่งตอนได้ `refresh-data`)

    topic: sync-permission

---

> อัปเดตจากโค้ด gumon-authentication-service@833b687 · 2026-10-05
