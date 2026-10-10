# Label Service

> ⚠️ **เลิกใช้แล้ว** — หน้าที่ของ Label Service ถูกรวมเข้า Unit Service · หน้านี้เก็บไว้อ้างอิงเท่านั้น
>
> ดูของปัจจุบัน (organization, organization type / tag และ role) ที่ [Unit Service](unitService.md)

Service สำหรับจัดการข้อมูล label ต่างๆ ซึ้งมีทั้ง

1. Organization
2. Business Unit
3. Position
4. Role
5. Custom
 
<br>

- [API Reference](#api-reference)
- [CLI Reference](gumoncli.md)
- [kafka consume Reference](#kafka-consume-reference)
- [kafka produce Reference](#kafka-produce-reference)

---

<br>
<br>

## API Reference
---

### Query

เป็น API ที่ใช้สำหรับการ Query ข้อมูลออกมา ไม่มีการแก้ไข Data

#### Get Label

API สำหรับการเรียกดูรายการข้อมูล label

    API name : getLabels


Response : labelSchema[]

labelSchema

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

---

#### Get Label By ID

API สำหรับการเรียกดูข้อมูล Label โดยอ้างอิงจาก ID

    API name : getLabelByID

Input Fields

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | String    | ID ของ label |


Response : labelSchema

labelSchema

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

---

---

### Mutation

เป็น API ที่ใช้สำหรับการแก้ไขข้อมูล

#### Create Label

API สำหรับการสร้าง Label

    API name : createLabel

Input Fields

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

Response : labelSchema

labelSchema

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

---
#### Update Label

API สำหรับการแก้ไขข้อมูล Label

    API name : updateLabel

Input Fields

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| id     | String    | id ของ label ที่ต้องหารแก้ไข  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

Response : labelSchema

labelSchema

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

---
#### delete Label

API สำหรับการลบ Label

    API name : deleteLabels

Input Fields

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _ids     | String[]    | list ID ของ label ที่ต้องการลบ  |


Response : DeleteStatus

DeleteStatus
| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| status     | ENUM    | SUCCESS, ERROR  |
| _id     | string[]    | list ของ ID  |

---

## kafka consume Reference

### Application
consume ข้อมูล application
    
    topic: sync-application

---

#### Add Application

รับข้อมูล Application เมื่อมีการสร้าง Application ใหม่ขึ้นมาในระบบ

    Action: ADD

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | string    | ชื่อ app  |
| attribute     | object    |   |

---

#### Update Application

รับข้อมูล Application เมื่อมีการ update Application

    Action: UPDATE

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | string    | ชื่อ app  |
| attribute     | object    |   |

---

#### Delete Application

รับข้อมูล Application เมื่อมีการ DELETE Application 

    Action: DELETE

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |

---

#### RefreshData

เมื่อมีคำสั่งนี้มา ให้ทำการส่งข้อมูลของตัวเอง อัตเดตขึ้น kafka

    Action: REFRESHDATA

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |

---

## Kafka Produce Reference

### Label

produce ข้อมูล label

    topic: sync-label

---

#### Add Label

ส่งข้อมูล label เมื่่อมีการสร้าง label ใหม่

    Action: ADD

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

---

#### Update Label

ส่งข้อมูล Label เมื่่อมีการแก้ไข

    Action: UPDATE

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |
| name     | String    | ชื่อ label ที่สร้าง  |
| description     | String    | คำอธิบาย  |

---

#### Delete Service

ส่งข้อมูล Label เมื่่อมีการลบ Label

    Action: DELETE

| key     |   Type    |  คำอธิบาย     |
| ------  | ------    | ------       |
| _id     | string    | id ที่ใช้อ้างอิง  |
| appKey     | string    | appKey  |

---
