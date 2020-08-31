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

#include <boost/filesystem.hpp>
#include <iostream>
#include <memory>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rcpputils/asserts.hpp"
#include "rcpputils/filesystem_helper.hpp"

#include "rosbag2_cpp/logging.hpp"
#include "rosbag2_cpp/reindexers/sequential_reindexer.hpp"


namespace rosbag2_cpp
{
namespace reindexers
{
namespace details
{
std::vector<std::string> resolve_relative_paths(
  const std::string & base_folder, std::vector<std::string> relative_files, const int version = 4)
{
  auto base_path = rcpputils::fs::path(base_folder);
  if (version < 4) {
    // In older rosbags (version <=3) relative files are prefixed with the rosbag folder name
    base_path = rcpputils::fs::path(base_folder).parent_path();
  }

  rcpputils::require_true(
    base_path.exists(), "base folder does not exist: " + base_folder);
  rcpputils::require_true(
    base_path.is_directory(), "base folder has to be a directory: " + base_folder);

  for (auto & file : relative_files) {
    auto path = rcpputils::fs::path(file);
    if (path.is_absolute()) {
      continue;
    }
    file = (base_path / path).string();
  }

  return relative_files;
}
}  // namespace details

std::string strip_parent_path(const std::string & relative_path)
{
  return rcpputils::fs::path(relative_path).filename().string();
}

SequentialReindexer::SequentialReindexer(
  std::unique_ptr<rosbag2_storage::StorageFactoryInterface> storage_factory,
  std::shared_ptr<SerializationFormatConverterFactoryInterface> converter_factory,
  std::unique_ptr<rosbag2_storage::MetadataIo> metadata_io)
: storage_factory_(std::move(storage_factory)),
  converter_(nullptr),
  metadata_io_(std::move(metadata_io)),
  converter_factory_(std::move(converter_factory))
{}

SequentialReindexer::~SequentialReindexer()
{
  reset();
}

void SequentialReindexer::reset()
{
  if (storage_) {
    storage_.reset();
  }
}


static bool comp_rel_file(const std::string &first_path, const std::string &second_path)
{
  std::regex regex_rule("(\\d+).db3$", std::regex_constants::ECMAScript);

  std::smatch first_match;
  std::smatch second_match;

  auto first_regex_good = std::regex_match(first_path, first_match, regex_rule);
  auto second_regex_good = std::regex_match(second_path, second_match, regex_rule);

  // Make sure the paths have regex matches
  if (!first_regex_good || !second_regex_good)
  {
    throw std::runtime_error("Malformed relative file name. Expected numerical identifier.");
  }

  // Convert database numbers to uint
  u_int32_t first_db_num = std::stoul(first_match.str(1), nullptr, 10);
  u_int32_t second_db_num = std::stoul(second_match.str(1), nullptr, 10);
  
  return first_db_num < second_db_num;

}

std::vector<std::string> SequentialReindexer::get_database_files(std::string base_folder)
{
  // Look in the uri directory to see what database files are there
  std::vector<std::string> output;
  for(auto& p_: boost::filesystem::directory_iterator(base_folder))
  {
    // We are ONLY interested in database files
    if (p_.path().extension() != ".db3")
    {
      continue;
    }

    output.emplace_back(p_.path().c_str());
  }

  // Sort relative file path by database number
  std::sort(output.begin(), output.end(), [](std::string a, std::string b){return comp_rel_file(a, b)});

  return output;

}

// std::string get_first_bagfile(const StorageOptions & storage_options)
// {
//   auto uri = storage_options.uri;

//   // First job is to find the database files in the bag


// }

void SequentialReindexer::open(
  const StorageOptions & storage_options)
{
  base_folder_ = storage_options.uri;
  auto files = get_database_files(base_folder_);
  if (files.empty()){
    ROSBAG2_CPP_LOG_ERROR("No database files found for reindexing. Abort");
    return;
  }
  // Since this is a reindexing operation, assume that there is no metadata.yaml file.
  // As such, ask the storage with the given URI for its metadata.
  storage_ = storage_factory_->open_read_only(
    files[0], storage_options.storage_id);
  if (!storage_) {
    throw std::runtime_error{"No storage could be initialized. Abort"};
  }
  metadata_ = storage_->get_metadata();
  metadata_.relative_file_paths.clear();  // The found path is going to be incorrect since we're accessing a random DB
  for (const auto & path : files) {
    auto cleaned_path = strip_parent_path(path);
    // std::cout << "Cleaned: " << cleaned_path << "\n";
    metadata_.relative_file_paths.push_back(cleaned_path);
  }
  file_paths_ = metadata_.relative_file_paths;
  current_file_iterator_ = file_paths_.begin();
  auto topics = metadata_.topics_with_message_count;
  if (topics.empty()) {
    ROSBAG2_CPP_LOG_WARN("No topics were listed in metadata.");
    return;
  }
  fill_topics_metadata();
}

void SequentialReindexer::fill_topics_metadata()
{
  rcpputils::check_true(storage_ != nullptr, "Bag is not open. Call open() before reading.");
  topics_metadata_.clear();
  topics_metadata_.reserve(metadata_.topics_with_message_count.size());
  for (const auto & topic_information : metadata_.topics_with_message_count) {
    topics_metadata_.push_back(topic_information.topic_metadata);
  }
}

// void SequentialReindexer::init_metadata()
// {
//   metadata_ = rosbag2_storage::BagMetadata{};
//   metadata_.storage_identifier = storage_->get_storage_identifier();
//   metadata_.starting_time = std::chrono::time_point<std::chrono::high_resolution_clock>(
//     std::chrono::nanoseconds::max());
//   // metadata_.relative_file_paths = {strip_parent_path(storage_->get_relative_file_path())};
//   for (const auto & path : metadata_.relative_file_paths) {
//     std::cout << path << "\n";
//   }
// }

void SequentialReindexer::aggregate_metadata()
{

}

void SequentialReindexer::reindex()
{
  ROSBAG2_CPP_LOG_INFO("Beginning Reindex Operation.");
  // init_metadata();  // Create a baseline to start from

  finalize_metadata();

  metadata_io_->write_metadata(base_folder_, metadata_);
  ROSBAG2_CPP_LOG_INFO("Reindexing operation completed.");

}

void SequentialReindexer::finalize_metadata()
{
  metadata_.bag_size = 0;

  for (const auto & path : metadata_.relative_file_paths) {
    const auto bag_path = rcpputils::fs::path{path};

    if (bag_path.exists()) {
      metadata_.bag_size += bag_path.file_size();
    }
  }

  // metadata_.topics_with_message_count.clear();
  // metadata_.topics_with_message_count.reserve(topics_names_to_info_.size());
  // metadata_.message_count = 0;

  // for (const auto & topic : topics_names_to_info_) {
  //   metadata_.topics_with_message_count.push_back(topic.second);
  //   metadata_.message_count += topic.second.message_count;
  // }
}
}  // namespace readers
}  // namespace rosbag2_cpp
