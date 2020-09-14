// Copyright 2018, Bosch Software Innovations GmbH.
// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ROSBAG2_CPP__REINDEXER_HPP
#define ROSBAG2_CPP__REINDEXER_HPP

#include <memory>

#include "rosbag2_cpp/converter_options.hpp"
#include "rosbag2_cpp/storage_options.hpp"
#include "rosbag2_cpp/visibility_control.hpp"

#include "rosbag2_storage/bag_metadata.hpp"
#include "rosbag2_storage/serialized_bag_message.hpp"
#include "rosbag2_storage/storage_filter.hpp"
#include "rosbag2_storage/topic_metadata.hpp"

// This is necessary because of using stl types here. It is completely safe, because
// a) the member is not accessible from the outside
// b) there are no inline functions.
#ifdef _WIN32
# pragma warning(push)
# pragma warning(disable:4251)
#endif

namespace rosbag2_cpp
{

namespace reindexer_interfaces
{
class BaseReindexerInterface;
} // namespace reindexer_interfaces

/** 
 * The reindexer attempts to construct a metadata file based on information contained in the bag.
 */
class ROSBAG2_CPP_PUBLIC Reindexer final
{
public:
  explicit Reindexer(std::unique_ptr<reindexer_interfaces::BaseReindexerInterface> reindexer_impl);

  ~Reindexer();

  // /**
  //  * Throws if file could not be opened.
  //  * This must be called before any other function is used.
  //  * The rosbag is automatically closed on destruction.
  //  *
  //  * If the `output_serialization_format` within the `converter_options` is not the same as the
  //  * format of the underlying stored data, a converter will be used to automatically convert the
  //  * data to the specified output format.
  //  * Throws if the converter plugin does not exist.
  //  *
  //  * \param storage_options Options to configure the storage
  //  */
  // void open(const StorageOptions & storage_options);

  /**
   * Attempts to create a metadata file from the opened bag
   *
   * \throws runtime_error if the Reader is not open.
   */
  void reindex(const StorageOptions & storage_options);

private:
  std::unique_ptr<reindexer_interfaces::BaseReindexerInterface> reindexer_impl;
};

} // namespace rosbag2_cpp

#ifdef _WIN32
# pragma warning(pop)
#endif

#endif  // ROSBAG2_CPP__REINDEXER_HPP