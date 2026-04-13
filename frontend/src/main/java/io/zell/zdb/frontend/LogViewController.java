/*
 * Copyright © 2021 Christopher Kujawa (zelldon91@gmail.com)
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
package io.zell.zdb.frontend;

import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.PersistedRecord;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.function.Consumer;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.*;
import javafx.stage.DirectoryChooser;

public class LogViewController implements Initializable {

  @FXML private TableView<ZeebeRecord> zeebeData;

  @FXML private Button findDataPath;

  @FXML private TextField dataPath;

  @FXML private TextField instanceKey;
  @FXML private TextField fromPosition;
  @FXML private TextField toPosition;
  private final DirectoryChooser directoryChooser = new DirectoryChooser();
  private ObservableList<ZeebeRecord> dataObservableList;

  @Override
  public void initialize(final URL url, final ResourceBundle resourceBundle) {
    // time to initialize fields
    final var userHome = System.getProperty("user.home");
    this.dataPath.setText(userHome);

    this.dataObservableList = FXCollections.observableList(new ArrayList<>());
    // put into the data table view
    this.zeebeData.setItems(this.dataObservableList);

    // define columns
    this.zeebeData
        .getColumns()
        .setAll(
            LogViewController.<Long>createTableColumn("Position", "position"),
            LogViewController.<Long>createTableColumn(
                "Source\nRecord\nPosition", "sourceRecordPosition"),
            LogViewController.<Long>createTableColumn("Timestamp", "timestamp"),
            LogViewController.<Long>createTableColumn("Key", "key"),
            LogViewController.<String>createTableColumn("Record\ntype", "recordType"),
            LogViewController.<String>createTableColumn("Value\ntype", "valueType"),
            LogViewController.<String>createTableColumn("Intent", "intent"),
            LogViewController.<String>createTableColumn("Rejection\ntype", "rejectionType"),
            LogViewController.<String>createTableColumn("Rejection\nreason", "rejectionReason"),
            LogViewController.<Long>createTableColumn("Request\nID", "requestId"),
            LogViewController.<Integer>createTableColumn("Request\nstream\nID", "requestStreamId"),
            LogViewController.<Integer>createTableColumn("Protocol\nversion", "protocolVersion"),
            LogViewController.<String>createTableColumn("Broker\nversion", "brokerVersion"),
            LogViewController.<Integer>createTableColumn("Record\nversion", "recordVersion"),
            LogViewController.<String>createTableColumn("Auth\ndata", "authData"),
            LogViewController.<String>createTableColumn("Record\nvalue", "recordValue"));

    // enable multi-selection
    this.zeebeData.getSelectionModel().setCellSelectionEnabled(true);
    this.zeebeData.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
    this.zeebeData.setOnKeyPressed(new TableViewKeyEventClipboardCopier<>(this.zeebeData));
  }

  private static <T> TableColumn<ZeebeRecord, T> createTableColumn(
      final String displayName, final String propertyName) {
    final TableColumn<ZeebeRecord, T> column = new TableColumn<ZeebeRecord, T>(displayName);
    column.setCellValueFactory(new PropertyValueFactory<ZeebeRecord, T>(propertyName));
    return column;
  }

  @FXML
  protected void onFindFile() {
    this.directoryChooser.setTitle("Zeebe data path");
    this.directoryChooser.setInitialDirectory(new File(System.getProperty("user.home")));

    final File file = this.directoryChooser.showDialog(this.findDataPath.getScene().getWindow());
    if (file != null) {
      this.dataPath.setText(file.getAbsolutePath());
      findLog();
    }
  }

  @FXML
  private void findLog() {
    if (this.dataPath.getText() == null || this.dataPath.getText().isBlank()) {
      return;
    }
    final var logContentReader = new LogContentReader(new File(this.dataPath.getText()).toPath());

    consumeValueFromTextField(this.instanceKey, logContentReader::filterForProcessInstance);
    consumeValueFromTextField(this.fromPosition, logContentReader::seekToPosition);
    consumeValueFromTextField(this.toPosition, logContentReader::limitToPosition);

    fillTableWithData(logContentReader);
  }

  private void consumeValueFromTextField(final TextField textField, final Consumer<Long> consumer) {
    if (textField.getText() != null && !textField.getText().isBlank()) {
      try {
        final long instanceKeyLong = Long.parseLong(textField.getText());
        consumer.accept(instanceKeyLong);
      } catch (final NumberFormatException nfe) {
        System.out.printf(
            "Expected to retrieve a number as instance key, but '%s' wasn't%n",
            textField.getText());
      }
    }
  }

  private void fillTableWithData(final LogContentReader logContentReader) {
    this.dataObservableList.clear();
    while (logContentReader.hasNext()) {
      final PersistedRecord next = logContentReader.next();

      if (next instanceof final ApplicationRecord applicationRecord) {
        for (final var r : applicationRecord.getEntries()) {
          final var zeebeRecord =
              new ZeebeRecord(
                  r.position(),
                  r.sourceRecordPosition(),
                  r.timestamp(),
                  r.key(),
                  r.recordType().name(),
                  r.valueType().name(),
                  r.intent().name(),
                  r.rejectionType().name(),
                  r.rejectionReason(),
                  r.requestId(),
                  r.requestStreamId(),
                  r.protocolVersion(),
                  r.brokerVersion(),
                  r.recordVersion(),
                  r.authData(),
                  r.recordValue().toString());
          this.dataObservableList.add(zeebeRecord);
        }
      }
    }
  }
}
