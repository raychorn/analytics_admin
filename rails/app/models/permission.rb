class Permission < ActiveRecord::Base
  has_and_belongs_to_many :roles

  # NOTE: the ability index number must not change, see seeds.rb
  CREATE_JOB = Permission.find_by_ability(1)
  CONFIGURE_USER_SETTINGS = Permission.find_by_ability(2)

  def self.search(search)
    if search
      where('name LIKE ?', "%#{search}%")
    else
      scoped
    end
  end
end
