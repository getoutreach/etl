module ETL::TmpUtil
  # creates a temp file in the specified tmp_dir/folder/user/current_date
  def self.by_day(tmp_dir, folder, part_name)
    # creating a daily file path so if the disk gets full its
    # easy to kill all the days except the current.
    date_path = DateTime.now.strftime('%Y_%m_%d')
    dir_path = "#{tmp_dir}/#{folder}/#{Etc.getlogin}/#{date_path}"
    FileUtils.makedirs(dir_path) unless Dir.exist?(dir_path)
    tmp_file = "#{dir_path}/#{part_name}_#{SecureRandom.hex(10)}"
    FileUtils.touch(tmp_file)
    tmp_file
  end
end

