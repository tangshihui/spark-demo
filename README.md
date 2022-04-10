#### build

```shell
./gradlew clean shadowJar
```


#### upload jar
```shell
 scp -i ~/.ssh/xxx.pem ./build/libs/spark-demo.jar <user>@<ip>:/home/user/test/spark-demo.jar
```