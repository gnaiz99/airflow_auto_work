import gspread

gs = gspread.service_account(r'C:\Users\Admin\Downloads\test-connect-ggsheet-454216-7e9f4c02bdda.json')

sht = gs.open_by_key('19AtCkKfcXNQp8d1hjPEK6wfGZhJv6cKxLI6YzP7cyQc')

print(sht.title)
# conect done #
