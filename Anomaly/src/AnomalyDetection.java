import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;


public class AnomalyDetection
{

	public static void main(String[] args) throws IOException
	{
		svm_model model = svmTrain();
		svm.svm_save_model("AnomalyDetectionModel", model);

	}
	
	private static svm_model svmTrain()
	{
		ArrayList<String> train = new ArrayList<String>();
		try 
		{
			BufferedReader br;
			br = new BufferedReader(new FileReader("training_data2"));
			String line;
			while ((line = br.readLine()) != null)
			{
			   train.add(line);
			}
			br.close();
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		
	    svm_problem prob = new svm_problem();
	    int dataCount = train.size();
	    prob.y = new double[dataCount];
	    prob.l = dataCount;
	    prob.x = new svm_node[dataCount][];     

	    int n_features = 0;
	    for (int i = 0; i < dataCount; i++){            
	        String[] features = train.get(i).split(",");
	        n_features = features.length;
	        prob.x[i] = new svm_node[n_features];
	        for (int j = 0; j < features.length; j++){
	            svm_node node = new svm_node();
	            node.index = j;
	            node.value = Double.parseDouble(features[j]);
	            System.out.print(String.valueOf(node.index) + ": " + String.valueOf(node.value)+"\n");
	            prob.x[i][j] = node;
	        }           
	        prob.y[i] = 1; // for one-class svm this parameter is omitted
	    }               

	    svm_parameter param = new svm_parameter();
	    param.probability = 1;
	    param.gamma = 0.5;
	    param.nu = 0.5;
	    param.svm_type = svm_parameter.ONE_CLASS;
	    param.kernel_type = svm_parameter.RBF;       
	    param.cache_size = 20000;
	    param.eps = 0.001; 
	    param.C = 1;
	    
	    svm_model model = svm.svm_train(prob, param);

	    return model;
	}

}
