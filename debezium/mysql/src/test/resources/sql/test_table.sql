create table if not exists test_table
(
    empno     varchar(20) not null,
    empname   varchar(20),
    deptno    int,
    birthdate date,
    salary    int

)
    partition by range (salary) (
        partition p1 values less than (1000),
        partition p2 values less than (2000),
        partition p3 values less than maxvalue
        );
