Facter.add("hadoop_storage_locations") do
        setcode do

            data_dir_path = "/data/"
            storage_locations = ""

            # We need to check /data/ exist
            if File.directory?(data_dir_path)

              # We assume all data directory will be a number
              Dir.foreach(data_dir_path) { |directory|
                  storage_locations += (data_dir_path + directory + ';') if directory =~ /\d+/
              }
            end

            # Return the list of storage locations for hadoop
            if storage_locations == ""
              storage_locations = "/mnt"
            end
            storage_locations
        end
end

