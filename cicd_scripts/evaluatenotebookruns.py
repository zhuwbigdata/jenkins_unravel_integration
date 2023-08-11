# evaluatenotebookruns.py
import unittest
import json
import glob
import os

class TestJobOutput(unittest.TestCase):

  test_output_path = '#ENV#'
  unravel_output_path = "#UNRAV#'

  def test_performance(self):
      path = self.test_output_path
      statuses = []

      for filename in glob.glob(os.path.join(path, '*.json')):
          print('Evaluating: ' + filename)
          data = json.load(open(filename))
          duration = data['execution_duration']
          if duration > 100000:
              status = 'FAILED'
          else:
              status = 'SUCCESS'

          statuses.append(status)

      self.assertFalse('FAILED' in statuses)

  def test_job_run(self):
      path = self.test_output_path
      statuses = []

      for filename in glob.glob(os.path.join(path, '*.json')):
          print('Evaluating: ' + filename)
          data = json.load(open(filename))
          status = data['state']['result_state']
          statuses.append(status)

      self.assertFalse('FAILED' in statuses)

  def test_unravel_check(self):
    path = self.unravel_output_path
    statuses = []

    filename = path + '/unravel.json'
    unrvel_md = None
    data = json.load(open(filename))
    if (data['insights'] == True) or (data['recommendations'] == True):
      msgfile =  path + '/unravel.md'
      with open(msgfile,'r') as file:
        unravel_md = file.read()
      status = 'FAILED'
    else:
      status = 'SUCCESS'
    statuses.append(status)

    self.assertFalse('FAILED' in statuses, msg = unravel_md)
    
if __name__ == '__main__':
  unittest.main()

  unittest.main(testRunner=xmlrunner.XMLTestRunner(output=out),
    failfast=False, buffer=False, catchbreak=False, exit=False)

  with open('TEST-report.xml', 'wb') as report:
    report.write(transform(out.getvalue()))
