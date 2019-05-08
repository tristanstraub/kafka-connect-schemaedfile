/*
 * Copyright 2016 David Tucker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbtucker.connect.file;

import java.util.Map;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaedFileSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(SchemaedFileSinkConnector.class);
  private SchemaedFileSinkConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new SchemaedFileSinkConnectorConfig(map);

  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return SchemaedFileSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
     ArrayList<Map<String,String>> configs = new ArrayList<>();
        for (File file : filelist) {
            Map<String,String> config = new HashMap<>();
            config.put("topic", topic);
            config.put("file",file.toString());
            configs.add(config);
        }
       return configs;
  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
  }

  @Override
  public ConfigDef config() {
    return SchemaedFileSinkConnectorConfig.conf();
  }
}
