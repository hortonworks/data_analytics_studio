# Query Language

Hive Studio comes with a expressive language to query the executed hive queries. This document describes the language in details.

This language is used to build the predicate which forms the basis of the query that is being used to fetch the details.

## Basic Search

Basic search does a full text search on the ```query``` field.

Eg:

```
create "hive studio" 
```

This searchs for two tokens ```create``` and ```hive studio```

```
create application_table NOW
```

This searchs for three tokens ```create``` ```application_table``` ```NOW```

Multiple string within a ```double``` or ```single quote``` is a grouped to token and the whitespaces inside will be considered while searching.



## Advanced Search

Basic form of the language is an ```expression```

```
expression = expression AND expression
expression = expression OR expression
expression = NOT expression
expression = simple_expression
```

Eg:

```
(name = 'beeline') AND (age > 30)
(name = 'beeline') OR (age > 30)
isMarried = NOT isSingle
```

So, the logical operators are ```AND | OR | NOT ``` and they are case-insensitive



```simple_expression``` can also of of type ``` comparison expression``` ,  ```between expression```, ```like expression```, ```in expression```, ```null expression```.

### Comparison Expression

Any kind of expression where we can compare two values. These expression can have a value of type ```string```, ```number```, ```date```, ```boolean```

Eg:

```
name = 'beeline'
salary > 1000 AND salary < 2000
```



### Between Expression

Any kind of expression where we can compare a value to a range.

Eg.

```
salary between 1000 and 2000
salary_date between NOW() and DATEADD(DAY, -5, NOW()) # salary_date in last 5 days 
```



### Like Expression

Checks if the string value contains the pattern. Check is case-insensitive.

Eg.

```
CURRENTUSER() like '%admin%'
```



### In expression

Checks if the value is contained in a set of values.

Eg:

```
application in ('beeline', 'hive studio', 'hivecli')
application not in ('beeline')
```



### Null Expression

Checks if the value is null.

Eg:

```
application IS NULL
application IS NOT NULL
```



### Functions

Note: Function names are case-insensitive.

**String functions**

| Name                                     | Description                              |
| ---------------------------------------- | ---------------------------------------- |
| concat(```string expression```, ```string expression```) | Concatenates two strings                 |
| substring(```string expression```, ```numeric expression```, ```numeric expression```) | Returns a substring                      |
| trim( ```TRIM SPECIFICATION``` ```string expression```) | Trims a string. ```trim specification``` can be ```LEADING | TRAILING | BOTH```. It is a optional parameter. Default is```BOTH```. |
| lower(```string expression```)           | Converts into a lower case string        |
| upper(```string expression```)           | Converts into a upper case string        |
| currentuser()                            | Return the current logged in user        |



**Numeric Functions**

| Name                                     | Description                              |
| ---------------------------------------- | ---------------------------------------- |
| length(```string expression```)          | Returns the length of the string         |
| abs(```numeric expression```)            | Returns the absolute value of the numeric value |
| sqrt(```numeric expression```)           | Returns the square root of the numeric value |
| mod(```numeric expression```, ```numeric expression```) | Returns the modulo                       |
| mem(```memory expression```)             | Returns the byte value of the memory expression |
| getdatepart(```date expression```, ```dateUnit```) | Returns the number extracted from the date expression and is determined by the ```dateUnit```.  Eg. GETDATEPART(NOW(), DAY) will return 22 |
| now()                                    | Retuns the current time in milliseconds  |

**Date Unit**

Date unit can be of the following type

```DAY```, ```MONTH```, ```YEAR```, ```HOUR```, ```MINUTE```, ```SECOND```



**Memory expression**

This is used to represent a memory value in easily comprehendable way. For example: 1 Kilo byte can be represented as ```1KB``` and 2.5 GB can be represented as ```2.5GB``` or ```2GB 512MB```

Units that can be represented are 

* ```B``` - Bytes
* ```KB``` - Kilo Bytes
* ```MB``` - Mega bytes
* ```GB``` - Giga bytes
* ```TB``` - Tera bytes
* ```PB``` - Peta bytes

Note: These units are case insensitive

Some examples

```
MEM(1Kb) = 1024
MEM(0.5Kb) = 512
MEM(1GB 1024MB) = 2147483648
```



**Date Functions**

| Name                                     | Description                              |
| ---------------------------------------- | ---------------------------------------- |
| currentdate()                            | Returns the current date                 |
| currenttime()                            | Returns the current time                 |
| currenttimestamp()                       | Returns the current timestamp            |
| todate(```string expression```, ```string expression```(optional)) | Returns the date value represented by the 1st string expression and 2nd String expression represents the date format. This is a optional parameter. Default value of the date format is 'yyyy-MM-dd hh:mm:ss' |
| dateadd(```date unit```, ```numeric value```, ```date value```) | Returns a new date which is a derived by added or substracted to the date value and the unit is represented by date unit. Eg: ```dateadd(YEAR, 5, current_date())``` will return same date after 5 years. |



### Operators

**Comparison Operators**

| Operator | Description           |
| -------- | --------------------- |
| =        | Equals operator       |
| <>       | Not equal operator    |
| >        | Greater than          |
| <        | Less than             |
| >=       | Greater than equal to |
| <=       | Less than equal to    |

**Binary Operators**

| operator | Description                        |
| -------- | ---------------------------------- |
| +        | Adds two numbers                   |
| -        | Substracts one number from another |
| *        | Multiply two numbers               |
| /        | Divide a number by another number  |
| \|\|     | Concats two strings into one       |



**Unary Operators**

| Operator | Description             |
| -------- | ----------------------- |
| +        | Positive value          |
| -        | Negates a numeric value |



### Literals

**Numeric literal**

Eg:

```
100 - Integer value
1,0000,000 - Integer value
1.25 - Decimal value
1,112.56 - Decimal value
```



**String Literal**

Any value within single quotes

Eg:

```
'hive studio' - String value
```



**Field Literal**

**field** is a value which represent the column name or the attribute which can be used in any expression.

This can be represented by ```unquoted string literal``` like ```application```,```name``` etc. or can be represented by ```quoted string``` like ```"column 0"```, ```"application name"``` etc.

Eg:

```
application
"column 0"
creation_date
```





## Complete Examples

```
(application in ('hive', 'tez') AND created_at < DATEADD(MONTH, -1, CURRENTDATE())) OR (status = 'submitted')
```



```
memory_used > MEM(5Mb) AND application like '%hive%'
```



More to be added...

