import java.io.IOException;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;

public class AnomalyDetection {
  public void run(double[] features) throws IOException {
    svm_model model = svm.svm_load_model("AnomalyDetectionModel");
    svm_node[] nodes = new svm_node[features.length];
    for (int i = 0; i < features.length; i++) {
      svm_node node = new svm_node();
      node.index = i;
      node.value = features[i];
      nodes[i] = node;
    }

    double result = svm.svm_predict(model, nodes);
    System.out.print("Anomaly Detection result: " + result);
  }

}
