class Importio
  class Response < Struct.new(:type, :data, :offset, :results, :cookies, :connector_version_guid, :connector_guid, :page_url)
    def initialize raw_data
      self.members.each do |member|
        key = member.to_s.gsub(/_(.)/) { $1.upcase if $1 } # camelize
        self[member] = raw_data[key] || raw_data[key.intern]
      end
    end

    def results
      return unless self.data
      self.data['results']
    end

    def message?
      self.type == 'MESSAGE'
    end
    def error?
      self.type == 'ERROR'
    end
  end
end
