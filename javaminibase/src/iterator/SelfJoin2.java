package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import index.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/** 
 *
 *  Implementation of the selfIEJoin as described in the paper 
 *  
 */

public class SelfJoin2  extends Iterator 
{
  private AttrType      _in1[]; 	// The array of types from the table on which we are performing the self join
  private   Iterator  outer;
  private   Tuple     inner_tuple;          
  private   FldSpec   perm_mat[];
  int [] P1; //first permutation
  int [] P2; //second permutation
  int [] B; //bit array
  private int eqOff;
  private ArrayList<Tuple> joinResult ; //will contain the final tuples

  /**constructor :
	initiliation the variables as with the NLJ
	initiliation of the iejoin as in the paper

   *@param in  Array containing field types of R.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param amt_of_mem  IN PAGES
   *@param am1  access method for left i/p to join
   *@param relationName  access heapfile for right i/p to join
   *@param outFilter   select expressions
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fields
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
   */
  public SelfJoin2( AttrType    in[],    
			   int     len_in1,           
			   short   t1_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,      
			   CondExpr outFilter[],         
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws 	IOException,
					NestedLoopException, 
					Exception,
					TupleUtilsException,
					SortException,
					UnknowAttrType,
					FieldNumberOutOfBoundException
    {
      
	//initialisation
      _in1 = new AttrType[in.length];
      System.arraycopy(in,0,_in1,0,in.length); 
      outer = am1;

      inner_tuple = new Tuple(); 

      ArrayList<Integer> col1 = new ArrayList<Integer>() ;
      ArrayList<Integer> col2 = new ArrayList<Integer>() ;
      ArrayList<Integer> col3 = new ArrayList<Integer>() ;
      ArrayList<Integer> col4 = new ArrayList<Integer>() ;
      ArrayList<Integer> tuplesList1 = new ArrayList<Integer>() ; //will contain the column of the first predicate
      ArrayList<Integer> tuplesList2 = new ArrayList<Integer>() ;  //will contain the column of the second predicate
      ArrayList<Tuple> AllTuples = new ArrayList<Tuple>() ; //will contain all the tuples of our single table
    
      
      AttrType[] inner_types = new AttrType[n_out_flds]; //nb of field outer relation
      short[]    t_size;
      
      perm_mat = proj_list;

      try {
	t_size = TupleUtils.setup_op_tuple(inner_tuple, inner_types,
					   in, len_in1,in, len_in1,
					   t1_str_sizes, t1_str_sizes,
					   proj_list, n_out_flds);
      } catch (TupleUtilsException e){
	throw new NestedLoopException(e,"TupleUtilsException is caught by SelfJoin2.java");
      }

      //initialising permutation and bit arrays
     
     Tuple current_tuple = null;
     int n = 0;
     try {
        while ((current_tuple = outer.get_next()) != null) {
          n = n + 1;
          Tuple new_tuple = new Tuple(current_tuple);
          col1.add(new_tuple.getIntFld(1));
          col2.add(new_tuple.getIntFld(2));
          col3.add(new_tuple.getIntFld(3));
          col4.add(new_tuple.getIntFld(4));
          AllTuples.add(new_tuple);
        }
      }
      catch (Exception e) {
        System.err.println (""+e);
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }

      int predcol1 = outFilter[0].operand1.symbol.offset ; //first column to sort
      int predcol2 = outFilter[1].operand1.symbol.offset ; //second column to sort

      // Sorting

      if (outFilter[0].op.toString() == "aopGT" || outFilter[0].toString() == "aopGE") //GT or GE
	      
	      {    
              switch (predcol1) {
                     case 1 : tuplesList1 = col1;
			break;
                     case 2 : tuplesList1 = col2;
			break;
                     case 3 : tuplesList1 = col3;
			break;
                     case 4 : tuplesList1 = col4;
			break;
                     default : System.out.println("Sorting error in SelfJoin2");
			break;
                     } 
              Collections.sort(tuplesList1);
   }
      else { if (outFilter[0].op.toString() == "aopLT" || outFilter[0].op.toString() == "aopLE") //LT or LE
	      
	      {
              switch (predcol1) {
                     case 1 : tuplesList1 = col1;
			break;
                     case 2 : tuplesList1 = col2;
			break;
                     case 3 : tuplesList1 = col3;
			break;
                     case 4 : tuplesList1 = col4;
			break;
                     default : System.out.println("Sorting error in SelfJoin2");
			break;
                     } 
              Collections.sort(tuplesList1,Collections.reverseOrder());
      } 
     }

      if (outFilter[1].op.toString() == "aopGT" || outFilter[1].toString() == "aopGE") //GT or GE
	      
	      {
              switch (predcol2) {
                     case 1 : tuplesList2 = col1;
			break;
                     case 2 : tuplesList2 = col2;
			break;
                     case 3 : tuplesList2 = col3;
			break;
                     case 4 : tuplesList2 = col4;
			break;
                     default : System.out.println("Sorting error in SelfJoin2");
			break;
                     }
              Collections.sort(tuplesList2);
   }
      else { if (outFilter[1].op.toString() == "aopLT" || outFilter[1].op.toString() == "aopLE") //LT or LE
	      {
              switch (predcol2) {
                     case 1 : tuplesList2 = col1;
			break;
                     case 2 : tuplesList2 = col2;
			break;
                     case 3 : tuplesList2 = col3;
			break;
                     case 4 : tuplesList2 = col4;
			break;
                     default : System.out.println("Sorting error in SelfJoin2");
			break;
                     } 
              Collections.sort(tuplesList2,Collections.reverseOrder());
      } 
     }


      // set eqOff variable
      if ( (outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") 
        && (outFilter[1].op.toString() == "aopGE" || outFilter[1].op.toString() == "aopLE") ) {eqOff = 0;}
      else {eqOff = 1;}

      //Permutation array

      P2 = new int[n];//permutation array
      B = new int[n]; //bit array

      Tuple t1 = null;
      Tuple t2 = null;

      for (int i=0; i<n ;i++)  { 
              for (int j=0; j<n ;j++) {
                  for (int k=0; k<n ; k++) { //get the corresponding tuples, gessing that each element is unique
		         if (AllTuples.get(k).getIntFld(predcol1) == tuplesList1.get(j)) { t1 = AllTuples.get(k); }
		         if (AllTuples.get(k).getIntFld(predcol2) == tuplesList2.get(i)) { t2 = AllTuples.get(k); }
                         }
                   if (t1 == t2) {
               	        P2[n-i-1]=j;
                 	break;
                   }
              }
     }

      //Bit array

      for (int i=0; i < n ; i++) { B[i]=0 ; }

      //applying the join

      joinResult = new ArrayList<Tuple>();
      int int_1;
      int int_2;
      
      Tuple tuple_1 = null;
      Tuple tuple_2 = null ;

      int position; //pos in the paper
      for (int i = 0; i < n ; i++)
        {
          position = P2[i] ; //we begin by the smallest of P2
          B[position] = 1 ; 

          for (int j = position+eqOff ; j < n ; j++)
            {
              if (B[j] == 1)
                {
                  int_1 = tuplesList1.get(j);
                  int_2 = tuplesList1.get(position);
                  for (int k=0; k<n ; k++) {
		         if (AllTuples.get(k).getIntFld(predcol1) == int_1) { tuple_1 = AllTuples.get(k); }
		         if (AllTuples.get(k).getIntFld(predcol1) == int_2) { tuple_2 = AllTuples.get(k); }
                         }
		  Projection.Join(tuple_1, _in1, 
					  tuple_2, _in1, 
					  inner_tuple, perm_mat, n_out_flds);
		  try{  
			Tuple x = new Tuple(inner_tuple);
                  	joinResult.add(x);
		  }
	      catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
              }
          }
      }

    }
  
  /**  
   *@return The next tuple in joinResult is returned
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions

   */
  public Tuple get_next()
    throws Exception
    {

     Tuple t = null;
  	try {
        while (joinResult.size() !=0) {
          t = joinResult.get(0);
          joinResult.remove(0);
          return t;
        }
	}
	catch (Exception f)
    {
        System.out.println("get_next error");
    } return null;
    } 
 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception IOException I/O error from lower layers
   *@exception JoinsException join error from lower layers
   *@exception IndexException index access error 
   */
  public void close() throws JoinsException, IOException,IndexException 
    {
      if (!closeFlag) {
	
	try {
	  outer.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "error in close");
	}
	closeFlag = true;
      }
    }
}