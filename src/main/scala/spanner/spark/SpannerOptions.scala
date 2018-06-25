/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spanner.spark

class SpannerOptions(@transient private val options: Map[String, String]) extends Serializable {

  import SpannerOptions._

  // table param has a higher precedence over path param
  // It's more of a convenience as path can make for better-looking Spark SQL apps
  lazy val table = options.get(TABLE_NAME).orElse(options.get(PATH)).get
  lazy val instanceId = options(INSTANCE_ID)
  lazy val databaseId = options(DATABASE_ID)

}
object SpannerOptions {
  val TABLE_NAME = "table"
  val PATH = "path"
  val INSTANCE_ID = "instanceId"
  val DATABASE_ID = "databaseId"
}
