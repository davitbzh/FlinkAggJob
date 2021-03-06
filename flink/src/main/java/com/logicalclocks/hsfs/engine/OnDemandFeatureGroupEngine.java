/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.OnDemandFeatureGroup;
import com.logicalclocks.hsfs.metadata.FeatureGroupApi;
import com.logicalclocks.hsfs.metadata.validation.ValidationType;
/*
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
*/

import java.io.IOException;

public class OnDemandFeatureGroupEngine extends FeatureGroupBaseEngine {

  private Utils utils = new Utils();

  private FeatureGroupApi featureGroupApi = new FeatureGroupApi();

  /*
  public void saveFeatureGroup(OnDemandFeatureGroup onDemandFeatureGroup)
      throws FeatureStoreException, IOException {
    Dataset<Row> onDemandDataset = null;
    if (onDemandFeatureGroup.getFeatures() == null) {
      onDemandDataset = SparkEngine.getInstance()
          .registerOnDemandTemporaryTable(onDemandFeatureGroup, "read_ondmd");
      onDemandFeatureGroup.setFeatures(utils.parseFeatureGroupSchema(onDemandDataset));
    }

    OnDemandFeatureGroup apiFg = featureGroupApi.save(onDemandFeatureGroup);
    onDemandFeatureGroup.setId(apiFg.getId());

    if (onDemandFeatureGroup.getValidationType() != ValidationType.NONE && onDemandDataset != null) {
      onDemandFeatureGroup.validateOnDemand(onDemandDataset);
    }
  }
  */
}
