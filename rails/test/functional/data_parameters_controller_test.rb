require 'test_helper'

class DataParametersControllerTest < ActionController::TestCase
  setup do
    @data_parameter = data_parameters(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:data_parameters)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create data_parameter" do
    assert_difference('DataParameter.count') do
      post :create, :data_parameter => @data_parameter.attributes
    end

    assert_redirected_to data_parameter_path(assigns(:data_parameter))
  end

  test "should show data_parameter" do
    get :show, :id => @data_parameter.to_param
    assert_response :success
  end

  test "should get edit" do
    get :edit, :id => @data_parameter.to_param
    assert_response :success
  end

  test "should update data_parameter" do
    put :update, :id => @data_parameter.to_param, :data_parameter => @data_parameter.attributes
    assert_redirected_to data_parameter_path(assigns(:data_parameter))
  end

  test "should destroy data_parameter" do
    assert_difference('DataParameter.count', -1) do
      delete :destroy, :id => @data_parameter.to_param
    end

    assert_redirected_to data_parameters_path
  end
end
