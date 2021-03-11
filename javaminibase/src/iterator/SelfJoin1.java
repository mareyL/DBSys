package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import index.*;
import java.util.ArrayList;
import java.io.*;

public class SelfJoin1   extends Iterator 
{

	  private AttrType   _in[];  // The array of types from the table on which we are performing the self join  
	  private   int      in_len; // The size of the previous array
	  private   Iterator outer; 
	  private   int      n_buf_pgs;      // Number of buffer pages available.
	  private   Tuple    inner_tuple;   
	  private   FldSpec  perm_mat[];
	  private   int      nOutFlds;
	  int [] P; // Permutation array
	  int [] B; // Position array
	  private int tuple_count; // Count of tuples in the result
	  private int eqOff;	// eqOff variable from pseudo-code
	  private ArrayList<Tuple> joinResult ; // Array to store and return the results
	  
	  
	  /**constructor
	   *Initialize the relation which is self joined, including relation type,
	   *@param in  Array containing field types of R.
	   *@param len_in  # of columns in R.
	   *@param t_str_sizes shows the length of the string fields.
	   *@param amt_of_mem  IN PAGES
	   *@param am1  access method for left i/p to join
	   *@param outFilter   select expressions
	   *@param proj_list shows what input fields go where in the output tuple
	   *@param n_out_flds number of outer relation fields
	   *@exception IOException some I/O fault
	   *@exception NestedLoopException exception from this class
	   * @throws SortException 
	 * @throws FieldNumberOutOfBoundException 
	   */
	  public SelfJoin1( AttrType    in[],    
				   int     len_in,           
				   short   t_str_sizes[],
			       int     amt_of_mem,        
				   Iterator     am1,          
				   CondExpr outFilter[],      
				   FldSpec   proj_list[],
				   int        n_out_flds
				   ) throws IOException,NestedLoopException, SortException, UnknowAttrType, FieldNumberOutOfBoundException
	    {
	      
	      _in = new AttrType[in.length];
	      
	      System.arraycopy(in,0,_in,0,in.length);
	      
	      in_len = len_in;
	      outer = am1;
	      inner_tuple = new Tuple();
	      ArrayList<Tuple> TuplesList = new ArrayList<Tuple>() ; // This array will be used for us to be able to deal  
	      														 // with tuples only, it will store all the tuples from
	      														 // the table on which we are performing the self join
	      
	      n_buf_pgs    = amt_of_mem;
	      
	      AttrType[] inner_types = new AttrType[n_out_flds];
	      short[]    t_size;
	      
	      perm_mat = proj_list;
	      nOutFlds = n_out_flds;
	      try {
		t_size = TupleUtils.setup_op_tuple(inner_tuple, inner_types,
						   in, len_in, in, len_in,
						   t_str_sizes, t_str_sizes,
						   proj_list, nOutFlds);
	      }
	      catch (TupleUtilsException e){
		  throw new NestedLoopException(e,"A TupleUtilsException was caught by SelfJoin.java during the initialisation phase");
	      }
	      
	     
	      
                          // Permutation and Bit array building //
	  
  // First step: sorting the relation

  Sort sorted_values = null;
 
  if (outFilter[0].op.toString() == "aopGT" || outFilter[0].toString() == "aopGE") // If the operation is 'strictly greater than' 
      																			   // or 'greater or equal to'
      {
      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending); 
      try {
	sorted_values = new Sort (in,(short)in_len, t_str_sizes,
			     (iterator.Iterator) outer, outFilter[0].operand1.symbol.offset, ascending, t_str_sizes[0], n_buf_pgs);
          }
      catch(Exception e)
	{
  	throw new SortException (e, "Failure in the sorting process.");
        } 
}
  
  else { if (outFilter[0].op.toString() == "aopLT" || outFilter[0].op.toString() == "aopLE") // If the operation is 'strictly less than'
	  																						 // or 'smaller or equal to' 
      {
      TupleOrder descending = new TupleOrder(TupleOrder.Descending);
      try {
	sorted_values = new Sort (in,(short)in_len, t_str_sizes,
			     (iterator.Iterator) outer, outFilter[0].operand1.symbol.offset, descending, t_str_sizes[0], n_buf_pgs);
          }
      catch(Exception e)
	{
  	throw new SortException (e, "Failure in the sorting process.");
        } 
  } 
 }

  // Second step: building the permutation array

  tuple_count = 0;              // Tuple count to be incremented 
  Tuple current_tuple = null;

  try {
    while ((current_tuple = sorted_values.get_next()) != null) {
      tuple_count = tuple_count + 1 ;
      Tuple new_tuple = new Tuple(current_tuple);
      TuplesList.add(new_tuple) ;
    }
  }
  catch (Exception e) {
    System.err.println (""+e);
    e.printStackTrace();
    Runtime.getRuntime().exit(1);
  }

  P = new int[tuple_count]; // Permutation array
  
  for (int i=0; i<tuple_count ;i++) { P[tuple_count-i-1]=i; } 
  

  // Third step: building the Bit array
  
  B = new int[tuple_count]; // Bit array
  
  for (int i=0; i < tuple_count ; i++) { B[i]=0 ; }

  
  							// Final result construction //
  
  // Creating array to store the results 
  joinResult = new ArrayList<Tuple>();
  Tuple tuple_left;
  Tuple tuple_right;
  
  // Setting the offset variable
  if (outFilter[0].op.toString() == "aopGE" || outFilter[0].op.toString() == "aopLE") 
  	   {eqOff = 0;}
  else {eqOff = 1;}
  
  // Fourth step: performing the iejoin

  int position;  // = pos in the given pseudo code
  for (int right_index = 0; right_index < tuple_count ; right_index++)
    {
      position = P[right_index] ; // Start scanning with the smallest value (w. r. t. the sorted array)
      
      B[position] = 1 ; // To remember where the smallest value (w. r. t. the sorted array) not in the bit
      					// array yet is
      for (int left_index = position+eqOff ; left_index < tuple_count ; left_index++)
        {
          if (B[left_index] == 1)
            {
              tuple_left = TuplesList.get(left_index);
              tuple_right = TuplesList.get(position);
	  Projection.Join(tuple_left, _in, 
				  tuple_right, _in, 
				  inner_tuple, perm_mat, nOutFlds);
	  try{  
		Tuple resulting_tuple = new Tuple(inner_tuple);
              	joinResult.add(resulting_tuple);
	  }
      catch(Exception e) {System.out.println("Error ocurred when adding the result tuples");}
          }
      }
  }

}
	  
	  /**  
	   *@return    The joined tuple 
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

			     Tuple tuple = null;
			  	try {
			        while (joinResult.size() !=0) {
			          tuple = joinResult.get(0);
			          joinResult.remove(0);
			          return tuple;
			           
			        }
				}
				catch (Exception f){ System.out.println("Get_next failed") ; } 
			  	
			  	return null;
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
		  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
		}
		closeFlag = true;
	      }
	    }
	}
