package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/** 
 *
 *  You will find in this file an implementation of the Inequality Join with two predicates 
 *  algorithm (task 2c) as described in the given paper.
 *  To be able to sort, and further access the data, we have decided to use ArrayList<Integer> type 
 *  to represent the different columns of our relations. Therefore, all our code assumes that every 
 *  tuple in every table is unique. This limitation could not be overcome without significant 
 *  changes to the Tuple class method, which we tried but could not succeed in doing.
 * 

 */

public class IEJoin  extends Iterator 
{
  private AttrType      _in1[],  _in2[]; // Tables description
  private   int        in1_len, in2_len;
  
  private   Iterator  table1iterator;
  private   Iterator  table2iterator;
  
  private   short t1_str_sizescopy[];
  
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  
  private   int        n_buf_pgs;        // Number of pages available in the buffer
  
  private   Tuple     Result_Tuple;      // Joined (resulting) tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  
  boolean predicate1_ascending, predicate2_ascending ; 
  
  int [] Permutation_1; 
  int [] Permutation_2;
  
  int [] Offset_predicate1;
  int [] Offset_predicate2;
  
  int [] B; // Position Byte-array
  private int m, n; // Sizes of both tables, named as in the paper
  private int eqOff;
  private ArrayList<Tuple> Join_Result ; // Will store the result
  

  /**constructor :
	initiliation the variables as with the NLJ
	initiliation of the iejoin as in the paper
	inequality join

   *Initialize the two relations which are joined, including relation type,
   *@param in1  Array containing field types of R.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param amt_of_mem  IN PAGES
   *@param am1  access method for left i/p to join
   *@param relationName  access hfapfile for right i/p to join
   *@param outFilter   select expressions
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
   */
  public IEJoin( AttrType    in1[],   
			   int     len_in1,          
			   short   t1_str_sizes[],
			   AttrType  in2[], 
			   int     len_in2,  
			   short   t2_str_sizes[],  
			   int     amt_of_mem,        
			   Iterator     am1,
			   Iterator     am2,          
			   String relationName,      
			   CondExpr outFilter[],
			   CondExpr RightFilter[],         
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws 	IOException,
					NestedLoopException, //a modifier
					Exception,
					TupleUtilsException,
					SortException,
					UnknowAttrType,
					FieldNumberOutOfBoundException
    {
      
      _in1 = new AttrType[in1.length];
      _in2 = new AttrType[in2.length];
      System.arraycopy(in1,0,_in1,0,in1.length); 
      System.arraycopy(in1,0,_in2,0,in2.length); 
      in1_len = len_in1; 
      in2_len = len_in2; 
      table1iterator = am1;
      table2iterator = am2;

      Result_Tuple = new Tuple(); // Result tuple
      OutputFilter = outFilter;

      ArrayList<Integer> Table1_attribute1 = new ArrayList<Integer>() ;
      ArrayList<Integer> Table1_attribute2 = new ArrayList<Integer>() ;
      ArrayList<Integer> Table1_attribute3 = new ArrayList<Integer>() ;
      ArrayList<Integer> Table1_attribute4 = new ArrayList<Integer>() ;

      ArrayList<Integer> Table2_attribute1 = new ArrayList<Integer>() ;
      ArrayList<Integer> Table2_attribute2 = new ArrayList<Integer>() ;
      ArrayList<Integer> Table2_attribute3 = new ArrayList<Integer>() ;
      ArrayList<Integer> Table2_attribute4 = new ArrayList<Integer>() ;

      ArrayList<Integer> table1_attribute_predicate1 = new ArrayList<Integer>() ; 
      ArrayList<Integer> table2_attribute_predicate1 = new ArrayList<Integer>() ; 
      ArrayList<Integer> table1_attribute_predicate2 = new ArrayList<Integer>() ;  
      ArrayList<Integer> table2_attribute_predicate2 = new ArrayList<Integer>() ;  
      
      ArrayList<Tuple> Table1_tuple = new ArrayList<Tuple>() ; //will contain all the tuples of table 1
      ArrayList<Tuple> Table2_tuple = new ArrayList<Tuple>() ; //will contain all the tuples of table 2
    
      n_buf_pgs    = amt_of_mem;
      
      AttrType[] Jtypes = new AttrType[n_out_flds]; 
      short[]    t_size;
      
      perm_mat = proj_list;
      nOutFlds = n_out_flds;

      try {
	t_size = TupleUtils.setup_op_tuple(Result_Tuple, Jtypes,
					   in1, len_in1,in2, len_in2,
					   t1_str_sizes, t2_str_sizes,
					   proj_list, nOutFlds);
      } catch (TupleUtilsException e){
	throw new NestedLoopException(e,"TupleUtilsException is caught by IEJoin.java");
      }


// First, we fill our ArrayList<Integer> to the corresponding column of the data
     
      
      // Table 1
     Tuple current_tuple = null;
     int m = 0;
     try {
        while ((current_tuple = table1iterator.get_next()) != null) {
          m = m + 1;
          Tuple auxiliary_tuple = new Tuple(current_tuple);
          Table1_attribute1.add(auxiliary_tuple.getIntFld(1));
          Table1_attribute2.add(auxiliary_tuple.getIntFld(2));
          Table1_attribute3.add(auxiliary_tuple.getIntFld(3));
          Table1_attribute4.add(auxiliary_tuple.getIntFld(4));
          Table1_tuple.add(auxiliary_tuple);
        }
      }
      catch (Exception e) {
        System.err.println (""+e);
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }

		// Table 2
     current_tuple = null;
     int n = 0;
     try {
        while ((current_tuple = table2iterator.get_next()) != null) {
          n = n + 1;
          Tuple auxiliary_tuple = new Tuple(current_tuple);
          Table2_attribute1.add(auxiliary_tuple.getIntFld(1));
          Table2_attribute2.add(auxiliary_tuple.getIntFld(2));
          Table2_attribute3.add(auxiliary_tuple.getIntFld(3));
          Table2_attribute4.add(auxiliary_tuple.getIntFld(4));
          Table2_tuple.add(auxiliary_tuple);
        }
      }
      catch (Exception e) {
        System.err.println (""+e);
        e.printStackTrace();
        Runtime.getRuntime().exit(1);
      }

     

      // First step: sorting the tables 

 // First, we build the columns corresponding to the needed data. We will then sort them depending on the predicates

     
     
     
switch (OutputFilter[0].operand1.symbol.offset) {
     case 1 : table1_attribute_predicate1 = Table1_attribute1;
break;
     case 2 : table1_attribute_predicate1 = Table1_attribute2;
break;
     case 3 : table1_attribute_predicate1 = Table1_attribute3;
break;
     case 4 : table1_attribute_predicate1 = Table1_attribute4;
break;
     default : System.out.println("Error when sorting in IEJoin: invalid left attribute number for the first predicate.");
break;
}



switch (OutputFilter[0].operand2.symbol.offset) {
     case 1 : table2_attribute_predicate1 = Table2_attribute1;
break;
     case 2 : table2_attribute_predicate1 = Table2_attribute2;
break;
     case 3 : table2_attribute_predicate1 = Table2_attribute3;
break;
     case 4 : table2_attribute_predicate1 = Table2_attribute4;
break;
     default : System.out.println("Error when sorting in IEJoin: invalid right attribute number for the first predicate. ");
break;
} 


switch (OutputFilter[1].operand1.symbol.offset) {
case 1 : table1_attribute_predicate2 = Table1_attribute1;
	break;
case 2 : table1_attribute_predicate2 = Table1_attribute2;
	break;
case 3 : table1_attribute_predicate2 = Table1_attribute3;
	break;
case 4 : table1_attribute_predicate2 = Table1_attribute4;
	break;
default : System.out.println("Error when sorting in IEJoin: invalid left attribute number for the second predicate. ");
	break;
} 


switch (OutputFilter[1].operand2.symbol.offset) {
case 1 : table2_attribute_predicate2 = Table2_attribute1;
	break;
case 2 : table2_attribute_predicate2 = Table2_attribute2;
	break;
case 3 : table2_attribute_predicate2 = Table2_attribute3;
	break;
case 4 : table2_attribute_predicate2 = Table2_attribute4;
	break;
default : System.out.println("**** Error when sorting on IeJoins_2c.java ****");
	break;     
}

 // Now, sorting those Arrays according to the predicates. 

if (OutputFilter[0].op.toString() == "aopGT" || OutputFilter[0].toString() == "aopGE") 
    
{    
	Collections.sort(table1_attribute_predicate1,Collections.reverseOrder());
    Collections.sort(table2_attribute_predicate1,Collections.reverseOrder());

    predicate1_ascending = false;
}
else { if (OutputFilter[0].op.toString() == "aopLT" || OutputFilter[0].op.toString() == "aopLE") 

{  Collections.sort(table1_attribute_predicate1);
	 Collections.sort(table2_attribute_predicate1);
	 predicate1_ascending = true;
 }


if (OutputFilter[1].op.toString() == "aopGT" || OutputFilter[1].toString() == "aopGE") 
	      
	      {  
              Collections.sort(table1_attribute_predicate2);
              Collections.sort(table2_attribute_predicate2);

              predicate2_ascending = true;
   }
      else { if (OutputFilter[1].op.toString() == "aopLT" || OutputFilter[1].op.toString() == "aopLE") //LT or LE
	      { 
    	  	Collections.sort(table1_attribute_predicate2,Collections.reverseOrder());
	      
              Collections.sort(table2_attribute_predicate2,Collections.reverseOrder());

              predicate2_ascending = false;
              }
           }
     

     
     

      // Second step: building the permutation arrays


 Permutation_1 = new int[m];
    
      
 Tuple tuple_pred1 = null;
 Tuple tuple_pred2 = null;

    for (int index_pred2=0; index_pred2<m ;index_pred2++)  { 
         for (int index_pred1=0; index_pred1<m ;index_pred1++) {
            for (int k=0; k<m ; k++) { 
		      if (Table1_tuple.get(k).getIntFld(OutputFilter[0].operand1.symbol.offset) == table1_attribute_predicate1.get(index_pred1)) { tuple_pred1 = Table1_tuple.get(k); }
		      if (Table1_tuple.get(k).getIntFld(OutputFilter[1].operand1.symbol.offset) == table1_attribute_predicate2.get(index_pred2)) { tuple_pred2 = Table1_tuple.get(k); }
                         }
               if (tuple_pred1 == tuple_pred2) {
               	     Permutation_1[index_pred2]=index_pred1;
               break;
                   }
              }
     }
    
    
Permutation_2 = new int[n];
  
tuple_pred1 = null; // Reseting the tuples before creating the second Permutation Table
tuple_pred2 = null;

      for (int i=0; i<n ;i++)  { 
              for (int j=0; j<n ;j++) {
                  for (int k=0; k<n ; k++) { 
		         if (Table2_tuple.get(k).getIntFld(OutputFilter[0].operand2.symbol.offset) == table2_attribute_predicate1.get(j)) { tuple_pred1 = Table2_tuple.get(k); }
		         if (Table2_tuple.get(k).getIntFld(OutputFilter[1].operand2.symbol.offset) == table2_attribute_predicate2.get(i)) { tuple_pred2 = Table2_tuple.get(k); }
                         }
                   if (tuple_pred1 == tuple_pred2) {
               	        Permutation_2[i]=j;
                 	break;
                   }
              }
     }
      
      
      // Third step: building the offsets arrays
      
      
 Offset_predicate1 = new int[m]; 
 
 int index_table2_start = 0;
    for (int index_table1=0; index_table1<m ;index_table1++)  { 
	    if (index_table1 > 0) {index_table2_start = Offset_predicate1[index_table1-1];} // We are working with sorted list: no need to go trough all the data
           for (int index_table2=index_table2_start; index_table2<n ;index_table2++) {
                    if (predicate1_ascending) 
                    {
	                   if (table1_attribute_predicate1.get(index_table1) <= table2_attribute_predicate1.get(index_table2)) {
        	       	        Offset_predicate1[index_table1]=index_table2;
        	                break;
        	           }
			   if (index_table2 == n-1 && table1_attribute_predicate1.get(index_table1) > table2_attribute_predicate1.get(index_table2)) {
                                Offset_predicate1[index_table1]=n;
				break;
			   }
                    }
                    else {
	                   if (table1_attribute_predicate1.get(index_table1) >= table2_attribute_predicate1.get(index_table2)) {
        	       	        Offset_predicate1[index_table1]=index_table2;
        	                break;
        	           }
			   if (index_table2 == n-1 && table1_attribute_predicate1.get(index_table1) < table2_attribute_predicate1.get(index_table2)) {
                                Offset_predicate1[index_table1]=n;
				break;
			   }
                    }      
              }
     }

      Offset_predicate2 = new int[m];
      

      index_table2_start = 0;
      for (int index_table1 =0; index_table1<m ;index_table1++)  { 
	      if (index_table1 > 0) {index_table2_start = Offset_predicate1[index_table1-1];} // We are working with sorted list: no need to go trough all the data
              for (int index_table2=index_table2_start; index_table2<n ;index_table2++) { 
		  if (predicate2_ascending)
                  {
	                   if (table1_attribute_predicate2.get(index_table1) <= table2_attribute_predicate2.get(index_table2)) {
	               	        Offset_predicate2[index_table1]=index_table2;
	                 	break;
	                   }
			   if (index_table2 == n-1 && table1_attribute_predicate2.get(index_table1) > table2_attribute_predicate2.get(index_table2)) {
                                Offset_predicate2[index_table1]=n;
				break;
			   }
                  }
	          else {
	                   if (table1_attribute_predicate2.get(index_table1) >= table2_attribute_predicate2.get(index_table2)) {
	               	        Offset_predicate2[index_table1]=index_table2;
	                 	break;
                           }
			   if (index_table2 == n-1 && table1_attribute_predicate2.get(index_table1) < table2_attribute_predicate2.get(index_table2)) {
                                Offset_predicate2[index_table1]=n;
				break;
			   }
		       }
              }
     }

   // Fourth step: initialization of the Bit array B and of the join_result list
      

 B = new int[n]; 
 for (int i=0; i < n ; i++) { B[i]=0 ; }
 
 Join_Result = new ArrayList<Tuple>();
 
 	// Fifth step: setting the equality offset variable
      
      if ( (outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") 
        && (outFilter[1].op.toString() == "aopGE" || outFilter[1].op.toString() == "aopLE") ) 
      {eqOff = 0;}
      else {eqOff = 1;}

   // Seventh and last step: building the result
      

int index_table1;
int index_table2;

int offset_table1;
int offset_table2;

Tuple tuple_table1 = null ;     
Tuple tuple_table2 = null;

for (int i = 0; i < m ; i++)
      {
  	  offset_table2 = Offset_predicate2[i];
  	  
  	  for (int j = 0 ; j < min(offset_table2, n) ; j++) 
  	  		{B[Permutation_2[j]] = 1;}
  	  
	  offset_table1 = Offset_predicate1[Permutation_1[i]];
	  for (int k = offset_table1+eqOff ; k < n ; k++) 
	  	{
        if (B[k] == 1)
                {
                index_table2 = table2_attribute_predicate1.get(k);
                index_table1 = table1_attribute_predicate2.get(i);
                for (int h=0; h<n ; h++) 
                	{ 
                	if (Table2_tuple.get(h).getIntFld(OutputFilter[0].operand2.symbol.offset) == index_table2) { tuple_table2 = Table2_tuple.get(h); }
                	if (Table1_tuple.get(h).getIntFld(OutputFilter[1].operand1.symbol.offset) == index_table1) { tuple_table1 = Table1_tuple.get(h); }
                	}
                Projection.Join(tuple_table2, _in1, 
                				tuple_table1, _in2, 
                				Result_Tuple, perm_mat, 
                				nOutFlds);
                try{  
                	Tuple auxiliary_tuple = new Tuple(Result_Tuple);
                  	Join_Result.add(auxiliary_tuple);
                	}
	            catch(Exception e) {System.out.println("Error occured when adding up together the result tuples");}
                 }
         }
      }
 } 
  
  /**  
   *@return The next tuple in Join_Result is returned
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

     Tuple current_tuple = null;
  	try {
        while (Join_Result.size() !=0) {
          current_tuple = Join_Result.get(0);
          Join_Result.remove(0);
          return current_tuple;
        }
	}
	catch (Exception f)
    {
        System.out.println("Issue in get_next");
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
	  table1iterator.close();
	  table2iterator.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "IeJoin.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
  public int min(int int1, int int2) { // A small function used in the last step above
		if (int1 < int2 ) {return int1 ; }
		else {return int2 ; }
	}
}
