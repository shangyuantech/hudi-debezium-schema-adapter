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


create table if not exists test_table_partition_list
(
    empno     varchar(20) not null,
    empname   varchar(20),
    deptno    int,
    birthdate date        not null,
    salary    int
)
    partition by list (deptno)
        (
        partition p1 values in (10, 20),
        partition p2 values in (30, 40),
        partition p3 values in (50)
        );

create table if not exists test_table_partition_hash
(
    empno     varchar(20) not null,
    empname   varchar(20),
    deptno    int,
    birthdate date        not null,
    salary    int
)
    partition by hash (year(birthdate))
        partitions 4;