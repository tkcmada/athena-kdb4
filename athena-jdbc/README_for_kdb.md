How to build in offline mode.
```
#in root directory of repository
#cp without overwriting to keep repository size small
cp -rp –no-clobber .m2/repository/* ~/.m2/repository/
cd ../athena-jdbc
mvn -o -llr clean install -DskipTests -Dmaven.test.skip -Dcheckstyle.skip
```

How to mvn test?
```
mvn -o -llr clean test -Dcheckstyle.skip '-Dtest=Kdb*Test'
```
You should ignore any failure/error in packages other than kdb.

How to create test data ?
```
download 32bit version binary and unarchvie it on your home directory.
$ unzip linuxx86.zip
$ cd q
$ ./l32/q -p 5001

t2:([] name:`symbol$(); bl:`boolean$(); bt:`byte$(); x:`int$(); lg:`long$(); r:`real$(); f:`float$(); date:`date$(); z:`timestamp$(); ts:`timespan$(); c:`char$(); g:`guid$() )
`t2 insert (`abc; 1b; 0x26; 100; 1000; 1.2e; 1.5; 2015.01.01; 2015.01.01D01:02:03.001002030; 01:02:03.001002000; "a"; (1?0Ng)[0] )
`t2 insert (`def; 0b; 0x04; 104; 1004; 1.4e; 1.4; 1970.01.04; 1970.01.04D00:00:00.004000000; 05:06:07.005006007; "d"; (1?0Ng)[0] )
`t2 insert (`   ; 0b; 0x00; 0Ni;  0Nj;  0Ne;  0n;        0Nd;                           0Np; 0Nn               ; " "; 0Ng        )
meta t2
t2
select name,t,ts,c,g from t2
select x from t2 where x >= 100i, x <= 104i
select x from t2 where (x >= 100i) and (x <= 104i)
select x from t2 where x in (100i, 104i)

t3:flip `name`str`lf`lb`li!(`abc`def`ghi;("xyz"; string "x"; "");1.0 1.1 1.5; (0x00; 0x01; 0x02); (0i; 1i; 100i))
meta t3
t3

t4: ([] c1:0 1; ll:(10 20; (0Nj; 40)); lb:((0x00; 0x01); (0x00; 0x03)); li:((0i; 1i); (0Ni; 3i)); ls:(`abc`def; (` ;`def)) ; lf:(1.0 1.1; 0n 1.5); lz:((1970.01.04D00:00:00.001002003 ; 1970.01.04D00:00:00.001002003) ; (0Np ; 1970.01.04D00:00:00.001002003)); date:(2020.01.01 2020.01.02) )
meta t4
t4

MarketBooks: ([] date:2020.01.01 2020.01.02; universal_id:( (1?0Ng)[0]; (1?0Ng)[0] ); sym:`USDJPY`USDJPY; version_id:`V1`V2; bid_prices:((0n; 100.02); (200.01; 200.02)); bid_amounts:((1000000; 2000000); (3000000; 4000000) ); ask_amounts:((1000001; 2000001); (3000001; 4000001) ) )
meta MarketBooks
MarketBooks

MarketBooksNoNull: ([] universal_id:( (1?0Ng)[0]; (1?0Ng)[0] ); sym:`USDJPY`USDJPY; version_id:`V1`V2; bid_prices:((100.01; 100.02); (200.01; 200.02)); bid_amounts:((1000000; 2000000); (3000000; 4000000) ); ask_amounts:((1000001; 2000001); (3000001; 4000001) ) )
meta MarketBooksNoNull
MarketBooksNoNull

t5:([] name:`USDJPY`EURUSD; str:("AAA"; "BBB"); liststr:(enlist "CCC"; ("AAA"; "BBB")) )

t6:([] name_group:`USDJPY`EURUSD )

t7:([] date:(2021.01.10;2021.01.11;2021.01.12;2021.01.13); sym:`USDJPY`USDJPY`USDJPY`USDJPY )
myFunc:{[date_from;date_to] select from t7 where date within (date_from; date_to) }
```

How to specify JDBC Connection String ?
```
kdb://jdbc:kdb:<ip>:<port>?user=<user>&password=<password>
```

(upstream only)
How to prepare offline build(only upstream side is required)
or
If you see error message "Could not resolve dependencies for project com.amazonaws:athena-jdbc:jar:1.0: Could not find artifact com.kx:jdbc:jar:0.1 in redshift (https://s3.amazonaws.com/redshift-maven-repository/release)"
```
#in repository root directory
mvn install:install-file -DgroupId=com.kx -DartifactId=jdbc -Dversion=0.1 -Dfile=.m2/repository/com/kx/jdbc/0.1/jdbc-0.1.jar -Dpackaging=jar   
cd athena-federation-sdk
mvn clean install -DskipTests -Dmaven.test.skip
cd ../athena-federation-integ-test
mvn clean install -DskipTests -Dmaven.test.skip
cd ../athena-jdbc
mvn clean install -DskipTests -Dmaven.test.skip -Dcheckstyle.skip
cd ..
cp -rp /workspace/m2-repository/* .m2/repository/
find .m2/repository -type f -and -not -name "*.jar" -and -not -name "*.pom"  -and -not -name "*.xml" | xargs rm -v
```

(upstream only)To compact git objects size(well, actually doesn't help a lot)
```
du -sh .git/objects
git filter-branch --tree-filter "rm -f -r .m2/repository/" HEAD
git \
  -c gc.pruneExpire=now \
  -c gc.worktreePruneExpire=now \
  -c gc.reflogExpire=now \
  -c gc.reflogExpireUnreachable=now \
  -c gc.rerereResolved=now \
  -c gc.rerereUnResolved=now \
  gc --aggressive
du -sh .git/objects

git remote set-url origin <new url>
git push  
```

(upstream only)To fetch and merge from upstream
```
git remote add upstream https://github.com/awslabs/aws-athena-query-federation.git
git fetch upstream
git merge v2021.51.1 (for example)
```

how to push using SSH key
```
cd ~/.ssh
ssh-keygen
#(no passphrase)
# and register ssh public key to remote repository
```
https://tks2.co.jp/2021/01/18/github-ssh/

