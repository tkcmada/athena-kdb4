 /*-
  * #%L
  * Amazon Athena Query Federation SDK
  * %%
  * Copyright (C) 2019 Amazon Web Services
  * %%
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *      http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * #L%
  */
 package com.amazonaws.athena.connector.lambda.data.writers.fieldwriters;

 import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
 import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
 import org.apache.arrow.vector.BitVector;
 import org.apache.arrow.vector.holders.NullableBitHolder;

 /**
  * Used to write a value and apply constraints for a particular column to the row currently being processed.
  * This interface enables the use of a pseudo-code generator for RowWriter which reduces object and branching
  * overhead when translating from your source system to Apache
  * <p>
  * For example of how to use this, see ExampleRecordHandler in athena-federation-sdk.
  *
  * @see FieldWriter
  */
 public class BitFieldWriter
         implements FieldWriter
 {
     private final NullableBitHolder holder = new NullableBitHolder();
     private final BitExtractor extractor;
     private final BitVector vector;
     private final ConstraintApplier constraint;

     /**
      * Creates a new instance of this FieldWriter and configures is such that writing a value required minimal
      * branching or secondary operations (metadata lookups, etc..)
      *
      * @param extractor The Extractor that can be used to obtain the value for the required column from the context.
      * @param vector The ApacheArrow vector to write the value to.
      * @param rawConstraint The Constraint to apply to the value when returning if the value was valid.
      */
     public BitFieldWriter(BitExtractor extractor, BitVector vector, ConstraintProjector rawConstraint)
     {
         this.extractor = extractor;
         this.vector = vector;
         if (rawConstraint != null) {
             constraint = (NullableBitHolder value) -> rawConstraint.apply(value.isSet == 0 ? null : value.value);
         }
         else {
             constraint = (NullableBitHolder value) -> true;
         }
     }

     /**
      * Attempts to write a value to the Apache Arrow vector provided at construction time.
      *
      * @param context The context (specific to the extractor) from which to extract a value.
      * @param rowNum The row to write the value into.
      * @return True if the value passed constraints and should be considered valid, False otherwise.
      */
     @Override
     public boolean write(Object context, int rowNum)
     {
         extractor.extract(context, holder);
         vector.setSafe(rowNum, holder);
         return constraint.apply(holder);
     }

     private interface ConstraintApplier
     {
         boolean apply(NullableBitHolder value);
     }
 }
