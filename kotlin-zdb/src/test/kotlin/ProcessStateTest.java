import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.util.FileUtil;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.state.instance.InstanceDetails;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ProcessStateTest {

  private static File tempDir = new File("/tmp/", "data-" + ThreadLocalRandom.current().nextLong());
  private static final BpmnModelInstance process = Bpmn.createExecutableProcess("process").startEvent().serviceTask("task").zeebeJobType("type").endEvent().done();;

  @Container
  public static ZeebeContainer zeebeContainer = new ZeebeContainer()
      .withFileSystemBind(tempDir.getPath(), "/usr/local/zeebe/data/", BindMode.READ_WRITE);

  private static DeploymentEvent deploymentEvent;
  private static ProcessInstanceEvent returnedProcessInstance;
  private static CountDownLatch jobLatch;

  @BeforeAll
  public static void setup() throws Exception {
    final ZeebeClient client =
        ZeebeClient.newClientBuilder()
            .gatewayAddress(zeebeContainer.getExternalGatewayAddress())
            .usePlaintext()
            .build();
    deploymentEvent = client.newDeployCommand().addProcessModel(process, "process.bpmn").send().join();

    returnedProcessInstance = client
        .newCreateInstanceCommand()
        .bpmnProcessId("process")
        .latestVersion()
        .send()
        .join();

    jobLatch = new CountDownLatch(1);
    client.newWorker().jobType("type").handler((jobClient, job) -> jobLatch.countDown()).open();
    jobLatch.await();
  }

// This is currently not working - it will cause java.nio.file.AccessDeniedException: /tmp/data--7809705097131595652/raft-partition/partitions/1/runtime/OPTIONS-000007
// Might be related to the test container usage
//  @AfterAll
//  public static void cleanup() throws Exception {
//    FileUtil.deleteFolderIfExists(tempDir.toPath());
//  }

  @Test
  public void shouldListProcesses() {
    // given

    // when
    final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(tempDir, "1"));
    final var processes = processState.listProcesses();

    // then
    final var processMeta = processes.get(0);
    final var returnedProcess = deploymentEvent.getProcesses().get(0);
    assertThat(processMeta.getBpmnProcessId()).isEqualTo(returnedProcess.getBpmnProcessId());
    assertThat(processMeta.getProcessDefinitionKey())
        .isEqualTo(returnedProcess.getProcessDefinitionKey());
    assertThat(processMeta.getResourceName()).isEqualTo(returnedProcess.getResourceName());
    assertThat(processMeta.getVersion()).isEqualTo(returnedProcess.getVersion());

    assertThat(processMeta.toString())
        .contains(returnedProcess.getBpmnProcessId())
        .contains(returnedProcess.getResourceName())
        .contains("" + returnedProcess.getVersion())
        .contains("" + returnedProcess.getProcessDefinitionKey());
  }

  @Test
  public void shouldGetProcessDetails() {
    // given

    // when
    final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(tempDir, "1"));
    final var returnedProcess = deploymentEvent.getProcesses().get(0);
    final var processDetails = processState
        .processDetails(returnedProcess.getProcessDefinitionKey());

    // then
    assertThat(processDetails.getBpmnProcessId()).isEqualTo(returnedProcess.getBpmnProcessId());
    assertThat(processDetails.getProcessDefinitionKey())
        .isEqualTo(returnedProcess.getProcessDefinitionKey());
    assertThat(processDetails.getResourceName()).isEqualTo(returnedProcess.getResourceName());
    assertThat(processDetails.getVersion()).isEqualTo(returnedProcess.getVersion());
    assertThat(processDetails.getResource()).isEqualTo(Bpmn.convertToString(process));

    assertThat(processDetails.toString())
        .contains(returnedProcess.getBpmnProcessId())
        .contains(returnedProcess.getResourceName())
        .contains("" + returnedProcess.getVersion())
        .contains("" + returnedProcess.getProcessDefinitionKey())
        .contains("xml");
  }


  @Test
  public void shouldNotFailOnNonExistingProcess() {
    // given

    // when
    final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(tempDir, "1"));
    final var processDetails = processState
        .processDetails(0xCAFE);

    // then
    assertThat(processDetails).isNull();
  }

  @Test
  public void shouldGetProcessInstanceDetails() {
    // given

    // when
    final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(tempDir, "1"));
    final var actualInstanceDetails = processState
        .instanceDetails(returnedProcessInstance.getProcessInstanceKey());

    // then
    assertThat(actualInstanceDetails).isNotNull();
    assertThat(actualInstanceDetails.getKey()).isEqualTo(returnedProcessInstance.getProcessInstanceKey());
    assertThat(actualInstanceDetails.getBpmnProcessId()).isEqualTo(returnedProcessInstance.getBpmnProcessId());
    assertThat(actualInstanceDetails.getProcessDefinitionKey())
        .isEqualTo(returnedProcessInstance.getProcessDefinitionKey());
    assertThat(actualInstanceDetails.getVersion()).isEqualTo(returnedProcessInstance.getVersion());
    assertThat(actualInstanceDetails.getProcessInstanceKey()).isEqualTo(returnedProcessInstance.getProcessInstanceKey());
    assertThat(actualInstanceDetails.getElementId()).isEqualTo("process");
    assertThat(actualInstanceDetails.getElementType()).isEqualTo(BpmnElementType.PROCESS);
    assertThat(actualInstanceDetails.getParentProcessInstanceKey()).isEqualTo(-1);
    assertThat(actualInstanceDetails.getParentElementInstanceKey()).isEqualTo(-1);
    assertThat(actualInstanceDetails.getFlowScopeKey()).isEqualTo(-1);
    assertThat(actualInstanceDetails.getChildren()).hasSize(1);

    assertThat(actualInstanceDetails.toString())
        .contains(returnedProcessInstance.getBpmnProcessId())
        .contains("" + returnedProcessInstance.getVersion())
        .contains("" + returnedProcessInstance.getProcessDefinitionKey())
        .contains(BpmnElementType.PROCESS.toString())
        .contains("process");
  }


  @Test
  public void shouldGetElementInstanceDetails() {
    // given
    final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(tempDir, "1"));
    final var processInstance = processState
        .instanceDetails(returnedProcessInstance.getProcessInstanceKey());

    // when
    InstanceDetails actualElementInstance = processState
        .instanceDetails(processInstance.getChildren().get(0));

    // then
    assertThat(actualElementInstance).isNotNull();
    assertThat(actualElementInstance.getBpmnProcessId()).isEqualTo(returnedProcessInstance.getBpmnProcessId());
    assertThat(actualElementInstance.getProcessDefinitionKey())
        .isEqualTo(returnedProcessInstance.getProcessDefinitionKey());
    assertThat(actualElementInstance.getVersion()).isEqualTo(returnedProcessInstance.getVersion());
    assertThat(actualElementInstance.getProcessInstanceKey()).isEqualTo(returnedProcessInstance.getProcessInstanceKey());
    assertThat(actualElementInstance.getElementId()).isEqualTo("task");
    assertThat(actualElementInstance.getElementType()).isEqualTo(BpmnElementType.SERVICE_TASK);
    assertThat(actualElementInstance.getParentProcessInstanceKey()).isEqualTo(-1);
    assertThat(actualElementInstance.getParentElementInstanceKey()).isEqualTo(-1);
    assertThat(actualElementInstance.getFlowScopeKey()).isEqualTo(processInstance.getKey());
    assertThat(actualElementInstance.getChildren()).isEmpty();

    assertThat(actualElementInstance.toString())
        .contains(returnedProcessInstance.getBpmnProcessId())
        .contains("" + returnedProcessInstance.getVersion())
        .contains("" + returnedProcessInstance.getProcessDefinitionKey())
        .contains(BpmnElementType.SERVICE_TASK.toString())
        .contains("task");
  }

  @Test
  public void shouldNotFailOnNonExistingProcessInstance() {
    // given

    // when
    final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(tempDir, "1"));
    final var actualInstanceDetails = processState
        .instanceDetails(0xCAFE);

    // then
    assertThat(actualInstanceDetails).isNull();
  }
}