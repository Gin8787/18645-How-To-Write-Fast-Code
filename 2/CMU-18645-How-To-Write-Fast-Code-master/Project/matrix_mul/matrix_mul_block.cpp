/*

    Copyright (C) 2011  Abhinav Jauhri (abhinav.jauhri@gmail.com), Carnegie Mellon University - Silicon Valley 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or (at your option) any later version.  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <omp.h>
#include <stdlib.h>
#include "matrix_mul.h"
#include <memory.h>
//#define block_size 8
namespace omp
{

void matrix_transpose(float *original_matrix, float *transposed_matrix, unsigned int sq_dimension)
{
	
#pragma omp parallel for
	for(unsigned int i = 0; i < sq_dimension; i++)
	  {
		for(unsigned int j = 0; j < sq_dimension; j++)
		{
			transposed_matrix[j*sq_dimension+i] = original_matrix[i*sq_dimension+j];
		}
	  }
}
  
void
  matrix_multiplication(float *sq_matrix_1, float *sq_matrix_2, float *sq_matrix_result, unsigned int sq_dimension )
  {
    float *sq_matrix_2_transposed; 
    sq_matrix_2_transposed = (float*)malloc(sizeof(float)*sq_dimension*sq_dimension);
    matrix_transpose(sq_matrix_2, sq_matrix_2_transposed, sq_dimension);
	memset(sq_matrix_result, 0, sizeof(float)*sq_dimension*sq_dimension);
	unsigned int block_size = sq_dimension/8;
	#pragma omp parallel for 
	for (unsigned int i = 0; i < sq_dimension; i+=block_size) {
		for(unsigned int j = 0; j < sq_dimension; j+=block_size) {
			for(unsigned int k = 0; k < sq_dimension; k++) {
				for(unsigned int m = i; m < ((i+block_size) < sq_dimension ? (i+block_size) : sq_dimension); m++) {
				    float local_sum = 0;
					for(unsigned int n = j; n < ((j+block_size) < sq_dimension ? (j+block_size) : sq_dimension); n++)
						//local_sum += sq_matrix_1[k*sq_dimension+n] * sq_matrix_2[n*sq_dimension+m];
						local_sum += sq_matrix_1[k*sq_dimension+n] * sq_matrix_2_transposed[m*sq_dimension+n];
					sq_matrix_result[k*sq_dimension+m] += local_sum;
				}
			}
	    }
     }
  }

} //namespace omp
