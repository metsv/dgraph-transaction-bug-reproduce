package test;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;


public class DGraphTransactionBugReproduceTest {

  private static GenericContainer dgraphServer;
  private static DgraphClient dgraphClient;
  private static ManagedChannel channel;
  private static volatile boolean isDgraphStarted;

  @Test
  public void addMissingNodes_whenTwoSimultaneousTransactionsTryToAddSameXids_thenOneTransactionMustFail() throws Exception {
    boolean secondTransactionAborted = false;

    DgraphClient.Transaction transaction1 = dgraphClient.newTransaction();
    DgraphClient.Transaction transaction2 = dgraphClient.newTransaction();

    String xid = "_:label_1 <xid> \"xid-1\" .\n";

    addMissingXid(xid, transaction1);
    addMissingXid(xid, transaction2);

    transaction2.commit();

    try {
      transaction1.commit();
    } catch (Exception e) {
      secondTransactionAborted = true;
    }

    assertTrue(secondTransactionAborted);

    DgraphClient.Transaction transaction3 = dgraphClient.newTransaction();
    assertEquals("{\"q\":[{\"count\":1}]}", transaction3.query("{q(func: eq(xid, \"xid-1\")) {count(uid)}}").getJson().toStringUtf8());
    transaction3.commit();
  }

  void addMissingXid(String insertXid , DgraphClient.Transaction transaction) {
    DgraphProto.Mutation mutation = DgraphProto.Mutation.newBuilder().setSetNquads(ByteString.copyFromUtf8(insertXid)).build();
    transaction.mutate(mutation);
  }

  @Before
  public void addSchema() {
    dgraphClient.alter(DgraphProto.Operation.newBuilder().setDropAll(true).build());
    String schema = "xid: string @index(hash) .\n";
    DgraphProto.Operation op = DgraphProto.Operation.newBuilder()
      .setSchema(schema).build();
    dgraphClient.alter(op);
  }

  @BeforeClass
  public static void startDgraphServer() throws Exception {
    if (!isDgraphStarted) {
      startServer();
      createClient();
      cleanupBeforeTermination();
      isDgraphStarted = true;
    }
  }

  private static void cleanupBeforeTermination() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (isDgraphStarted) {
        try {
          channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {}
        if (dgraphServer.isRunning()) {
          dgraphServer.stop();
        }
      }
    }));
  }

  private static void startServer() throws InterruptedException {
    /******************* With dgraph version 1.0.3 provided test case transaction support works************************/
   /*
    dgraphServer = new GenericContainer("dgraph/dgraph:v1.0.3")
      .withCommand("/bin/sh", "-c", "dgraph zero & (sleep 1; dgraph server --memory_mb 2048 --zero localhost:5080)")
      .withExposedPorts(8080, 9080);
    */
    dgraphServer = new GenericContainer("dgraph/dgraph:v1.0.7")
      .withCommand("/bin/sh", "-c", "dgraph zero & (sleep 1; dgraph server --lru_mb 2048 --zero localhost:5080)")
      .withExposedPorts(8080, 9080);

    dgraphServer.setPortBindings(Arrays.asList("8180:8080", "9180:9080"));
    dgraphServer.start();
    Thread.sleep(5000);
  }

  private static void createClient() {
    channel = ManagedChannelBuilder.forAddress(dgraphServer.getContainerIpAddress(),
      dgraphServer.getMappedPort(9080)).usePlaintext(true).build();
    DgraphGrpc.DgraphBlockingStub blockingStub = DgraphGrpc.newBlockingStub(channel);
    dgraphClient = new DgraphClient(Collections.singletonList(blockingStub));
  }
}

