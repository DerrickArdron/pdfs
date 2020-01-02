import pyautogui, time

pyautogui.PAUSE = 1
pyautogui.FAILSAFE = True

# Move to the Adelphi Icon
pyautogui.moveTo(532, 897)
pyautogui.click(clicks = 2)
time.sleep(5)
# hit the sign  in button
#pyautogui.moveTo(1411, 762)
#pyautogui.click()
# Organisations
pyautogui.moveTo(532, 897)         
pyautogui.click()
# Lodges/Chapters
pyautogui.moveTo(134,762)         
pyautogui.click()         