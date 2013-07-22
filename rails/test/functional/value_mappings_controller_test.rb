require 'test_helper'

class ValueMappingsControllerTest < ActionController::TestCase
  setup do
    @value_mapping = value_mappings(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:value_mappings)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create value_mapping" do
    assert_difference('ValueMapping.count') do
      post :create, :value_mapping => @value_mapping.attributes
    end

    assert_redirected_to value_mapping_path(assigns(:value_mapping))
  end

  test "should show value_mapping" do
    get :show, :id => @value_mapping.to_param
    assert_response :success
  end

  test "should get edit" do
    get :edit, :id => @value_mapping.to_param
    assert_response :success
  end

  test "should update value_mapping" do
    put :update, :id => @value_mapping.to_param, :value_mapping => @value_mapping.attributes
    assert_redirected_to value_mapping_path(assigns(:value_mapping))
  end

  test "should destroy value_mapping" do
    assert_difference('ValueMapping.count', -1) do
      delete :destroy, :id => @value_mapping.to_param
    end

    assert_redirected_to value_mappings_path
  end
end
