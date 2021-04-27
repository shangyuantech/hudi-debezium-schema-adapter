# drop table test_table;
create table if not exists test_table
(
    empno     varchar(20) not null,
    empname   varchar(20),
    deptno    int,
    birthdate date,
    salary    int,
    PRIMARY KEY (empno, salary)
)
    partition by range (salary) (
        partition p1 values less than (1000),
        partition p2 values less than (2000),
        partition p3 values less than maxvalue
        );

delete from test_table;

insert into test_table values('001', 'mike1', 130, '1989-08-04', 10000);
insert into test_table values('002', 'mike2', 131, '1990-08-04', 11000);
insert into test_table values('003', 'mike3', 132, '1991-08-04', 12000);
insert into test_table values('004', 'mike4', 133, '1992-08-04', 13000);

commit;