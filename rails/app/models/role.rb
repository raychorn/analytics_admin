class Role < ActiveRecord::Base
  has_and_belongs_to_many :permissions
  has_and_belongs_to_many :users

  attr_accessible :name, :permission_ids

  def permissions_names
    permissions.collect{ |p| p.name }.join ', '
  end

  def self.search(search)
    if search
      where('name LIKE ?', "%#{search}%")
    else
      scoped
    end
  end
end
