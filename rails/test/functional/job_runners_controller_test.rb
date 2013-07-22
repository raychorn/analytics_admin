require 'test_helper'

class JobRunnersControllerTest < ActionController::TestCase
  setup do
    @job_runner = job_runners(:one)
  end

  test "should get index" do
    get :index
    assert_response :success
    assert_not_nil assigns(:job_runners)
  end

  test "should get new" do
    get :new
    assert_response :success
  end

  test "should create job_runner" do
    assert_difference('JobRunner.count') do
      post :create, :job_runner => @job_runner.attributes
    end

    assert_redirected_to job_runner_path(assigns(:job_runner))
  end

  test "should show job_runner" do
    get :show, :id => @job_runner.to_param
    assert_response :success
  end

  test "should get edit" do
    get :edit, :id => @job_runner.to_param
    assert_response :success
  end

  test "should update job_runner" do
    put :update, :id => @job_runner.to_param, :job_runner => @job_runner.attributes
    assert_redirected_to job_runner_path(assigns(:job_runner))
  end

  test "should destroy job_runner" do
    assert_difference('JobRunner.count', -1) do
      delete :destroy, :id => @job_runner.to_param
    end

    assert_redirected_to job_runners_path
  end
end
