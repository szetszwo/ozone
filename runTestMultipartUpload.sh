set -x
TEST_CLASS=TestMultipartUpload
TEST_METHOOD=testMultipartParallel

MVN_SKIP_OZONE="-DskipDocs -Dmaven.javadoc.skip=true -Dlicense.skip=true -Dfindbugs.skip=true -Denforcer.skip=true -Dskip.installnpm -Dskip.installnpx -Dskip.installyarn -Dskip.npm -Dskip.npx -Dskip.yarn -DskipShade"

for i in `seq 1 ${1}`;
do
  mvn package ${MVN_SKIP_OZONE} -am -pl :ozone-integration-test \
    -Dmaven.test.redirectTestOutputToFile=false \
    -Dtest=${TEST_CLASS}\#${TEST_METHOOD} \
    >&1 | tee ${TEST_METHOOD}.${i}.log
done

