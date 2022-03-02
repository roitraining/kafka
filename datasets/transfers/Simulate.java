import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Random;

public class Simulate {

   public void print(int naccounts, int ntrans, String transactionsFilename, String edgesFilename) throws java.io.IOException {
      Random rnd = new Random();

      // create a random set of variances for each account
      double[] vars = new double[naccounts];
      for (int i=0; i < vars.length; ++i){
         vars[i] = 1 + rnd.nextInt(3); // 1-4 close relationships
      }

      PrintWriter tw = new PrintWriter(new FileWriter(transactionsFilename));
      int[][] edges = new int[naccounts][naccounts];
      for (int i=0; i < ntrans; ++i){
          int from = rnd.nextInt(naccounts);
          // related with Gaussian probability to "nearby" accounts
          int to = modulo(from + rnd.nextGaussian()*vars[from], naccounts);
          if ( from != to ){
            edges[from][to] ++;
            int amount = Math.abs( (int)(rnd.nextGaussian() * 10000) + 3000 );
            tw.println("A" + from + " A" + to + " " + amount);
          } 
      }
      tw.close();
      System.out.println("Wrote out " + transactionsFilename);

      // typically, one would use Hadoop to process the input file
      // to create the output edges file, but here, we'll just write it out
      // since we have all the data anyway ...
      PrintWriter ew = new PrintWriter(new FileWriter(edgesFilename));
      for (int i=0; i < edges.length; ++i){
          String from = "A" + i;
          for (int j=0; j < edges[i].length; ++j){
              if ( edges[i][j] > 0 ){
                 String to = "A" + j;
                 ew.println(from + " " + to + "\t" + edges[i][j]);
              }
          }
      }
      ew.close(); 
      System.out.println("Wrote out " + edgesFilename);
   }
   private static int modulo(double f, int N){
      // java's % operator returns -ve numbers of -ve inputs, so roll our own
      int a = (int)(f);
      return (a%N + N)%N;
   }

   public static void main(String[] args) throws Exception {
       Simulate s = new Simulate();
       s.print(10, 1000, "transfers.txt","sample.txt");
   }

}
