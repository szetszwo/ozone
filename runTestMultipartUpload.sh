LOOP_N=${1}
test -z "${LOOP_N}" && LOOP_N=20

TEST_METHOD=${2}
test -z "${TEST_METHOD}" && TEST_METHOD=testMultipartParallel

TEST_CLASS=${3}
test -z "${TEST_CLASS}" && TEST_CLASS=TestMultipartUpload

MVN_SKIP_OZONE="-DskipDocs -Dmaven.javadoc.skip=true -Dlicense.skip=true -Dfindbugs.skip=true -Denforcer.skip=true -Dskip.installnpm -Dskip.installnpx -Dskip.installyarn -Dskip.npm -Dskip.npx -Dskip.yarn -DskipShade"

set -x
for i in $(seq 1 ${LOOP_N});
do
  mvn package ${MVN_SKIP_OZONE} -am -pl :ozone-integration-test \
    -Dmaven.test.redirectTestOutputToFile=false \
    -Dtest=${TEST_CLASS}\#${TEST_METHOD} \
    >&1 | tee ${TEST_METHOD}.${i}.log
done

