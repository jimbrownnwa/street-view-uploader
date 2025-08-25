import subprocess
import sys
import time
import os
import signal

def run_with_timeout():
    print("Starting batch processing with timeout protection...")
    
    # Start the main process
    process = subprocess.Popen([sys.executable, "run_large_batch.py"], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.STDOUT,
                              text=True,
                              bufsize=1)
    
    start_time = time.time()
    last_output_time = time.time()
    completion_detected = False
    
    try:
        # Monitor output in real-time
        while process.poll() is None:
            line = process.stdout.readline()
            if line:
                print(line.rstrip())
                last_output_time = time.time()
                
                # Detect completion signals
                if "Large batch processing complete!" in line or "Progress file cleaned up." in line:
                    completion_detected = True
                    print("COMPLETION DETECTED - Waiting 10 seconds then forcing exit...")
                    time.sleep(10)  # Give it a chance to exit naturally
                    break
            
            # Check for timeout conditions
            current_time = time.time()
            total_runtime = current_time - start_time
            idle_time = current_time - last_output_time
            
            # Force exit if hanging after completion or running too long
            if completion_detected or total_runtime > 1800 or idle_time > 300:  # 30 min max, 5 min idle
                print("FORCING TERMINATION...")
                break
                
            time.sleep(0.1)
        
        # Force kill if still running
        if process.poll() is None:
            print("Process still running - force killing...")
            try:
                process.terminate()
                time.sleep(2)
                if process.poll() is None:
                    process.kill()
            except:
                pass
        
        # Get any remaining output
        remaining_output, _ = process.communicate(timeout=5)
        if remaining_output:
            print(remaining_output)
            
    except Exception as e:
        print(f"Error during execution: {e}")
        try:
            process.kill()
        except:
            pass
    
    print("Batch processing wrapper complete!")
    return 0

if __name__ == "__main__":
    sys.exit(run_with_timeout())