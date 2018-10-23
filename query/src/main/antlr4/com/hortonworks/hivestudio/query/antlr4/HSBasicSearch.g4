grammar HSBasicSearch;

/*
 * PARSER rules
 */
queryExpression
    : (whitespace|dqString|sqString|uqString)+ EOF
    ;

dqString
    : DQ STRING (whitespace|STRING)* DQ
    ;


sqString
    : SQ STRING (whitespace|STRING)* SQ
    ;

uqString
    : STRING
    ;

whitespace
    : WHITESPACE
    ;


/*
 * LEXER rules
 */
STRING
    : [a-zA-Z0-9_-]+
    ;

DQ
    : '"'
    ;

SQ
    : '\''
    ;

WHITESPACE
    : [ ]+
    ;

REST
    : [\t\r\n]+ -> skip
    ;
