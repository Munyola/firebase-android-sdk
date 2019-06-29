// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.android.datatransport.runtime.scheduling.persistence;

import android.database.sqlite.SQLiteDatabase;
import java.util.List;

class DatabaseMigrationClient {
  private final List<Migration> incrementalMigrations;

  DatabaseMigrationClient(List<Migration> incrementalMigrations) {
    this.incrementalMigrations = incrementalMigrations;
  }

  void upgrade(SQLiteDatabase db, int fromVersion, int toVersion) {
    for (Migration m : incrementalMigrations) {
      m.upgrade(db, fromVersion, toVersion);
    }
  }

  public interface Migration {
    void upgrade(SQLiteDatabase db, int fromVersion, int toVersion);
  }
}
