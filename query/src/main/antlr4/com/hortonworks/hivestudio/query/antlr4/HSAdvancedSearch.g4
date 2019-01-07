/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
grammar HSAdvancedSearch;

/*
 * PARSER rules
 */
queryExpression
    : expression
    ;

expression
    : expression K_OR expression
    | expression K_AND expression
    | K_NOT expression
    | simpleExpression
    ;

simpleExpression
    : LPAREN expression RPAREN
    | simpleConditionExpression
    ;

simpleConditionExpression
    : comparisonExpression
    | betweenExpression
    | likeExpression
    | inExpression
    | nullExpression
    ;

fieldExpression
    : unQuotedFieldExpression
    | quotedFieldExpression
    ;

unQuotedFieldExpression
    : IDENTIFICATION_VARIABLE
    ;

quotedFieldExpression
    : QUOTED_IDENTIFICATION_VARIABLE
    ;

comparisonExpression
    : stringExpression comparisonOperator stringExpression
    | booleanExpression (EQ | NE) booleanExpression
    | datetimeExpression comparisonOperator datetimeExpression
    | arithmeticExpression comparisonOperator arithmeticExpression
    ;


betweenExpression
   : arithmeticExpression (K_NOT)? K_BETWEEN arithmeticExpression K_AND arithmeticExpression
   | stringExpression (K_NOT)? K_BETWEEN stringExpression K_AND stringExpression
   | datetimeExpression (K_NOT)? K_BETWEEN datetimeExpression K_AND datetimeExpression
   ;

likeExpression
   : stringExpression (K_NOT)? K_LIKE patternValue
   ;

patternValue
   : STRINGLITERAL
   ;

inExpression
   : fieldExpression (K_NOT)? K_IN LPAREN (inTerm (COMMA inTerm)*) RPAREN
   ;

inTerm
   : literal
   ;

nullExpression
   : (fieldExpression) K_IS (K_NOT)? K_NULL
   ;

comparisonOperator
    : EQ
    | GT
    | GTE
    | LT
    | LTE
    | NE
    ;

stringExpression
    : fieldExpression
    | STRINGLITERAL
    | functionsReturningStrings
    | stringExpression CONCAT stringExpression
    | LPAREN stringExpression CONCAT stringExpression RPAREN
    ;

functionsReturningStrings
   : F_CONCAT       LPAREN stringExpression COMMA stringExpression RPAREN
   | F_SUBSTRING    LPAREN stringExpression COMMA arithmeticExpression COMMA arithmeticExpression RPAREN
   | F_TRIM         LPAREN (trimSpecification)?  stringExpression RPAREN
   | F_LOWER        LPAREN stringExpression RPAREN
   | F_UPPER        LPAREN stringExpression RPAREN
   | F_CURRENTUSER  LPAREN RPAREN
   ;

trimSpecification
   : K_LEADING
   | K_TRAILING
   | K_BOTH
   ;

booleanExpression
    : fieldExpression
    | booleanLiteral
    ;

booleanLiteral
    : TRUE
    | FALSE
    ;

datetimeExpression
    : fieldExpression
    | functionsReturningDatetime
    ;

functionsReturningDatetime
    : F_CURRENTDATE         LPAREN RPAREN
    | F_CURRENTTIME         LPAREN RPAREN
    | F_CURRENTTIMESTAMP    LPAREN RPAREN
    | F_TODATE              LPAREN stringExpression (COMMA stringExpression)? RPAREN
    | F_DATEADD             LPAREN dateUnit COMMA arithmeticExpression COMMA datetimeExpression RPAREN
    ;

dateUnit
    : (U_DAY | U_MONTH | U_YEAR | U_HOUR | U_MINUTE | U_SECOND)
    ;

arithmeticExpression
    : arithmeticExpression ( MULTI | DIV | PLUS | MINUS ) arithmeticExpression
    | arithmeticFactor
    ;

arithmeticFactor
    : (PLUS | MINUS)? arithmatic_primary
    ;

arithmatic_primary
    : fieldExpression
    | numericLiteral
    | LPAREN arithmeticExpression RPAREN
    | functionsReturningNumerics
    ;

functionsReturningNumerics
   : F_LENGTH       LPAREN stringExpression RPAREN
   | F_ABS          LPAREN arithmeticExpression RPAREN
   | F_SQRT         LPAREN arithmeticExpression RPAREN
   | F_MOD          LPAREN arithmeticExpression COMMA arithmeticExpression RPAREN
   | F_MEM          LPAREN memoryExpression RPAREN
   | F_GETDATEPART  LPAREN datetimeExpression COMMA dateUnit RPAREN
   | F_NOW          LPAREN RPAREN
   ;

memoryExpression
    : (integerLiteral memoryUnit)+
    ;

numericLiteral
    : integerLiteral
    | decimalLiteral
    ;

integerLiteral
    : INT
    ;

decimalLiteral
    : FLOAT
    ;

literal
    : STRINGLITERAL
    | INT
    | FLOAT
    ;

memoryUnit
    : U_B | U_KB | U_MB | U_GB | U_TB | U_PB
    ;

/*
 * LEXER rules
 */

// Keywords
K_AND                 : A N D;
K_OR                  : O R;
K_NOT                 : N O T;
K_LIKE                : L I K E;
K_BETWEEN             : B E T W E E N;
K_IN                  : I N;
K_IS                  : I S;
K_NULL                : N U L L;
K_LEADING             : L E A D I N G;
K_TRAILING            : T R A I L I N G;
K_BOTH                : B O T H;


// Functions
F_CONCAT              : C O N C A T;
F_SUBSTRING           : S U B S T R I N G;
F_TRIM                : T R I M;
F_LOWER               : L O W E R;
F_UPPER               : U P P E R;
F_CURRENTUSER         : C U R R E N T U S E R;

F_CURRENTDATE         : C U R R E N T D A T E;
F_CURRENTTIME         : C U R R E N T T I M E;
F_CURRENTTIMESTAMP    : C U R R E N T T I M E S T A M P;
F_TODATE              : T O D A T E;
F_NOW                 : N O W;
F_DATEADD             : D A T E A D D;

F_LENGTH              : L E N G T H;
F_ABS                 : A B S;
F_SQRT                : S Q R T;
F_MOD                 : M O D;
F_MEM                 : M E M;
F_GETDATEPART         : G E T D A T E P A R T;


U_YEAR                : Y E A R;
U_MONTH               : M O N T H;
U_DAY                 : D A Y;
U_HOUR                : H O U R;
U_MINUTE              : M I N U T E;
U_SECOND              : S E C O N D;

U_B                   : B;
U_KB                  : K B;
U_MB                  : M B;
U_GB                  : G B;
U_TB                  : T B;
U_PB                  : P B;


// Operators
TRUE                  : T R U E;
FALSE                 : F A L S E;

PLUS                  : '+';
MINUS                 : '-';
MULTI                 : '*';
DIV                   : '/';

GT                    : '>';
GTE                   : '>=';
LT                    : '<';
LTE                   : '<=';
NE                    : '<>';
EQ                    : '=';

CONCAT                : '||';



LPAREN                : '(';
RPAREN                : ')';
COMMA                 : ',';


INT
    : DIGITS ((COMMA|DIGITS)* (DIGITS)+)*
    ;

FLOAT
    : INT '.' DIGITS+
    ;

DIGITS
    : '0'..'9'
    ;

IDENTIFICATION_VARIABLE
    : ('a' .. 'z' | 'A' .. 'Z' | '_') ('a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_')*
    ;

QUOTED_IDENTIFICATION_VARIABLE
    : ('"' (~ ('\\' | '"'))*? '"')
    ;

STRINGLITERAL
   : ('\'' (~ ('\\' | '"'))*? '\'')
   ;

WS
    : [ \t\r\n]+ -> skip
    ;

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
