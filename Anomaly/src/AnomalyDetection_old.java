import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Random;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

public class AnomalyDetection_old {
	
	public static double randInt(int min, int max) {

	    // NOTE: Usually this should be a field rather than a method
	    // variable so that it is not re-seeded every call.
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum/100.0;
	}

	public static void main(String[] args)
	{
		
		/*for (int i = 0; i < 100; i++)
		{
			String line = String.format("%d 1:%f 2:%f 3:%f 4:%f 5:%f 6:%f 7:%f 8:%f 9:%f 10:%f 11:%f 12:%f 13:%f", i%2, 0.0, randInt(1,10), randInt(0, 10), 0.0, randInt(0, 20), randInt(0, 50), randInt(0, 10), randInt(0, 10), 0.0, randInt(0, 15), randInt(0, 10), randInt(0, 10), randInt(0, 10));
			System.out.print(line);
			System.out.print("\n");
		}*/
		
		/*for (int i = 0; i < 100; i++)
		{
			String line = String.format("%f %f %f %f %f %f", randInt(0, 10), randInt(0, 20), randInt(0, 50), randInt(0, 10), randInt(0, 10), randInt(0, 15));
			System.out.print(line);
			System.out.print("\n");
		}*/
		
		
		
		svm_model model = svmTrain();
		System.out.print("*******************************"+"\n");
		
		// 2, 3, 5, 6, 6, 9, 1, 11, 65, 32, 2, 5, 4
		// 1, 3, 6, 6, 6, 9, 1, 11, 66, 32, 2, 5, 4
		// 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88, 88
		double[] features = new double[]{2, 3.0008, 5, 6, 6, 9.009};
		features = new double[]{randInt(0, 10), randInt(0, 20), randInt(0, 50), randInt(0, 10), randInt(0, 10), randInt(0, 15)};
		svm_node[] nodes = new svm_node[features.length];
	    for (int i = 0; i < features.length; i++)
	    {
	        svm_node node = new svm_node();
	        node.index = i;
	        node.value = features[i];
	        //System.out.print(String.valueOf(node.index) + ": " + String.valueOf(node.value)+"\n");

	        nodes[i] = node;
	    }
	    
		/*double a = Math.random()*3;
		double b = Math.random()*40;
		double c = Math.random()*0;
		double d = Math.random()*6;
		double e = Math.random()*22;
		double f = Math.random()*40;
		double g = Math.random()*8;
		double h = Math.random()*77;
		double i = Math.random()*0;
		double j = Math.random()*3;
		double k = Math.random()*24;
		double l = Math.random()*10;
		double m = Math.random()*4;

		
		features = new double[]{a, b, c, d, e, f, g, h, i, j, k, l, m};
		
		nodes = new svm_node[features.length];
	    for (int n = 0; n < features.length; n++)
	    {
	        svm_node node = new svm_node();
	        node.index = n;
	        node.value = features[n];

	        nodes[n] = node;
	    }*/
        
	    
	    double result = svm.svm_predict(model, nodes);
	    
		System.out.print(result);
		
	}
	
	public double evaluate(double[] features, svm_model model) 
	{
	    svm_node[] nodes = new svm_node[features.length-1];
	    for (int i = 1; i < features.length; i++)
	    {
	        svm_node node = new svm_node();
	        node.index = i;
	        node.value = features[i];

	        nodes[i-1] = node;
	    }

	    int totalClasses = 2;       
	    int[] labels = new int[totalClasses];
	    svm.svm_get_labels(model,labels);

	    double[] prob_estimates = new double[totalClasses];
	    double v = svm.svm_predict_probability(model, nodes, prob_estimates);

	    for (int i = 0; i < totalClasses; i++){
	        System.out.print("(" + labels[i] + ":" + prob_estimates[i] + ")");
	    }
	    System.out.println("(Actual:" + features[0] + " Prediction:" + v + ")");            

	    return v;
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
		
		
		/*train = new ArrayList<String>();
		for (int n = 0; n < 100; n++)
		{
			double a = Math.random()*3;
			double b = Math.random()*40;
			double c = Math.random()*0;
			double d = Math.random()*6;
			double e = Math.random()*22;
			double f = Math.random()*40;
			double g = Math.random()*8;
			double h = Math.random()*77;
			double i = Math.random()*0;
			double j = Math.random()*3;
			double k = Math.random()*24;
			double l = Math.random()*10;
			double m = Math.random()*4;
			String line = String.valueOf(a) + " " + String.valueOf(b) + " "  +
						  String.valueOf(c) + " " + String.valueOf(d) + " "  + 
						  String.valueOf(e) + " " + String.valueOf(f) + " "  +
						  String.valueOf(g) + " " + String.valueOf(h) + " "  +
						  String.valueOf(i) + " " + String.valueOf(j) + " "  +
						  String.valueOf(k) + " " + String.valueOf(l) + " "  +
						  String.valueOf(m);
			train.add(line);
		}*/
		
		
	    svm_problem prob = new svm_problem();
	    int dataCount = train.size();
	    prob.y = new double[dataCount];
	    prob.l = dataCount;
	    prob.x = new svm_node[dataCount][];     

	    int n_features = 0;
	    for (int i = 0; i < dataCount; i++){            
	        String[] features = train.get(i).split(" ");
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
	    param.gamma = 0.5;//0.0078125;//1.0/n_features;
	    param.nu = 0.5;//0.5;
	    param.svm_type = svm_parameter.ONE_CLASS;
	    param.kernel_type = svm_parameter.RBF;       
	    param.cache_size = 20000;
	    param.eps = 0.001; 
	    param.C = 1;//0.03125;
	    

	    //double[] target = new double[prob.l];
	    //svm.svm_cross_validation(prob, param, 5, target); 
	    svm_model model = svm.svm_train(prob, param);

	    return model;
	}

}
