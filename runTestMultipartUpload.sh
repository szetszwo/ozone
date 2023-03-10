set -x
TEST_CLASS=TestMultipartUpload
TEST_METHOOD=testMultipartParallel
for i in `seq 1 99`;
do
  mvn package $MVN_SKIP_OZONE -am -pl :ozone-integration-test \
    -Dmaven.test.redirectTestOutputToFile=false \
    -Dtest=${TEST_CLASS}\#${TEST_METHOOD} \
    >&1 | tee ${TEST_METHOOD}.${i}.log
done

