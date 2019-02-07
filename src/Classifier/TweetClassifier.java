package Classifier;

import com.aliasi.classify.LMClassifier;
import com.aliasi.stats.MultivariateEstimator;

import com.aliasi.classify.Classification;
import com.aliasi.classify.Classified;
import com.aliasi.classify.DynamicLMClassifier;

import com.aliasi.lm.NGramProcessLM;

import java.io.*;

public class TweetClassifier {

    static DynamicLMClassifier<NGramProcessLM> trainedClassificator;
    LMClassifier<NGramProcessLM, MultivariateEstimator> mClassifier;

    public TweetClassifier(String datasetPath) throws IOException, ClassNotFoundException {

        // load the dataset if avaiable, if not throw an exception
        FileInputStream fileInputStream = new FileInputStream(datasetPath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        ObjectInputStream objectInputStream = new ObjectInputStream(bufferedInputStream);
        Object object = objectInputStream.readObject();
        objectInputStream.close();
        mClassifier = (LMClassifier<NGramProcessLM, MultivariateEstimator>)object;
    }

    public String evaluateText(String text){
        Classification classification = mClassifier.classify(text);
        return classification.bestCategory();
    }

    static void trainDataset(String datasetPath, String[] mCategories, int nGram) throws IOException {

        trainedClassificator = DynamicLMClassifier.createNGramProcess(mCategories,nGram);
        System.out.println("\nTraining.");

        String line = "", clss="";

        FileReader fileReader = new FileReader(datasetPath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        while((line = bufferedReader.readLine()) != null) {
            String[] element = line.split(",", 2);
            if(element[0].contains("0")) {
                clss = "neg";
            }else{
                clss = "pos";
            }
            Classification classification = new Classification(clss);
            Classified<CharSequence> classified = new Classified<CharSequence>(element[1],classification);
            trainedClassificator.handle(classified);
        }

        // Always close files.
        bufferedReader.close();

        FileOutputStream fout = new FileOutputStream("classifier.lpc");
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        trainedClassificator.compileTo(oos);
        oos.close();
    }


    void evaluateDataset(String datasetpath) throws IOException {
        System.out.println("\nEvaluating.");
        int numTests = 0;
        int numCorrect = 0;

        String line = "", categ="";
        // FileReader reads text files in the default encoding.
        FileReader fileReader = new FileReader(datasetpath);

        // Always wrap FileReader in BufferedReader.
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        while((line = bufferedReader.readLine()) != null) {
            numTests += 1;
            System.out.print("Num of test: " + numTests +" \r");
            String[] element = line.split(",", 2);
            if(element[0].contains("0")) {
                categ = "neg";
            }else{
                categ = "pos";
            }

            if(evaluateText(element[1]).equals(categ))
                numCorrect += 1;
        }

        bufferedReader.close();

        System.out.println("  # Test Cases=" + numTests);
        System.out.println("  # Correct=" + numCorrect);
        System.out.println("  % Correct="
                + ((double)numCorrect)/(double)numTests);
    }

    public static void main(String args[]){
        String text = " oh yeah summer starts  WOOOOOOO";
        try {
            TweetClassifier pol = new TweetClassifier("/path/to/classifier.lpc");
            System.out.println(text);
            System.out.println(pol.evaluateText(text));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
