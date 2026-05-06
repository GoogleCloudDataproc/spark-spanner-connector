-- Sccsid:     @(#)dss.ddl	2.1.8.1
CREATE TABLE NATION ( N_NATIONKEY  INT64 NOT NULL,
                      N_NAME       STRING(25) NOT NULL,
                      N_REGIONKEY  INT64 NOT NULL,
                      N_COMMENT    STRING(152)
                    ) PRIMARY KEY (N_NATIONKEY);

CREATE TABLE REGION ( R_REGIONKEY  INT64 NOT NULL,
                      R_NAME       STRING(25) NOT NULL,
                      R_COMMENT    STRING(152)
                    ) PRIMARY KEY (R_REGIONKEY);

CREATE TABLE PART  ( P_PARTKEY     INT64 NOT NULL,
                     P_NAME        STRING(55) NOT NULL,
                     P_MFGR        STRING(25) NOT NULL,
                     P_BRAND       STRING(10) NOT NULL,
                     P_TYPE        STRING(25) NOT NULL,
                     P_SIZE        INT64 NOT NULL,
                     P_CONTAINER   STRING(10) NOT NULL,
                     P_RETAILPRICE NUMERIC NOT NULL,
                     P_COMMENT     STRING(23) NOT NULL
                   ) PRIMARY KEY (P_PARTKEY);

CREATE TABLE SUPPLIER ( S_SUPPKEY     INT64 NOT NULL,
                        S_NAME        STRING(25) NOT NULL,
                        S_ADDRESS     STRING(40) NOT NULL,
                        S_NATIONKEY   INT64 NOT NULL,
                        S_PHONE       STRING(15) NOT NULL,
                        S_ACCTBAL     NUMERIC NOT NULL,
                        S_COMMENT     STRING(101) NOT NULL
                      ) PRIMARY KEY (S_SUPPKEY);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INT64 NOT NULL,
                        PS_SUPPKEY     INT64 NOT NULL,
                        PS_AVAILQTY    INT64 NOT NULL,
                        PS_SUPPLYCOST  NUMERIC  NOT NULL,
                        PS_COMMENT     STRING(199) NOT NULL
                      ) PRIMARY KEY (PS_PARTKEY);

CREATE TABLE CUSTOMER ( C_CUSTKEY     INT64 NOT NULL,
                        C_NAME        STRING(25) NOT NULL,
                        C_ADDRESS     STRING(40) NOT NULL,
                        C_NATIONKEY   INT64 NOT NULL,
                        C_PHONE       STRING(15) NOT NULL,
                        C_ACCTBAL     NUMERIC   NOT NULL,
                        C_MKTSEGMENT  STRING(10) NOT NULL,
                        C_COMMENT     STRING(117) NOT NULL
                      ) PRIMARY KEY (C_CUSTKEY);

CREATE TABLE ORDERS  ( O_ORDERKEY       INT64 NOT NULL,
                       O_CUSTKEY        INT64 NOT NULL,
                       O_ORDERSTATUS    STRING(1) NOT NULL,
                       O_TOTALPRICE     NUMERIC NOT NULL,
                       O_ORDERDATE      DATE NOT NULL,
                       O_ORDERPRIORITY  STRING(15) NOT NULL,
                       O_CLERK          STRING(15) NOT NULL,
                       O_SHIPPRIORITY   INT64 NOT NULL,
                       O_COMMENT        STRING(79) NOT NULL
                     ) PRIMARY KEY (O_ORDERKEY);

-- Child Table: LINEITEM (Interleaved in ORDERS)
CREATE TABLE LINEITEM ( O_ORDERKEY      INT64   NOT NULL,
                        L_PARTKEY       INT64   NOT NULL,
                        L_SUPPKEY       INT64   NOT NULL,
                        L_LINENUMBER    INT64   NOT NULL,
                        L_QUANTITY      NUMERIC NOT NULL,
                        L_EXTENDEDPRICE NUMERIC NOT NULL,
                        L_DISCOUNT      NUMERIC NOT NULL,
                        L_TAX           NUMERIC NOT NULL,
                        L_RETURNFLAG    STRING(1) NOT NULL,
                        L_LINESTATUS    STRING(1) NOT NULL,
                        L_SHIPDATE      DATE    NOT NULL,
                        L_COMMITDATE    DATE    NOT NULL,
                        L_RECEIPTDATE   DATE    NOT NULL,
                        L_SHIPINSTRUCT  STRING(25) NOT NULL,
                        L_SHIPMODE      STRING(10) NOT NULL,
                        L_COMMENT       STRING(44) NOT NULL
                    ) PRIMARY KEY (O_ORDERKEY, L_LINENUMBER),
                      INTERLEAVE IN PARENT ORDERS ON DELETE CASCADE;

