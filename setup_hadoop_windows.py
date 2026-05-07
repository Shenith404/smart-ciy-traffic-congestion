"""
Setup script to download Hadoop winutils for Windows
This resolves the HADOOP_HOME error when running PySpark on Windows
"""
import os
import urllib.request
import sys

# Try multiple sources and versions
DOWNLOAD_SOURCES = [
    {
        "name": "kontext-tech (recommended)",
        "base": "https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-{version}/bin",
        "versions": ["3.3.1", "3.2.0", "3.0.0"]
    },
    {
        "name": "steveloughran",
        "base": "https://github.com/steveloughran/winutils/raw/master/hadoop-{version}/bin",
        "versions": ["3.0.0", "2.7.1", "2.8.3"]
    },
    {
        "name": "cdarlint",
        "base": "https://github.com/cdarlint/winutils/raw/master/hadoop-{version}/bin",
        "versions": ["3.2.2", "3.3.0", "3.0.0"]
    }
]

def get_download_attempts():
    """Generate all download URLs to try"""
    attempts = []
    for source in DOWNLOAD_SOURCES:
        for version in source["versions"]:
            base_url = source["base"].format(version=version)
            attempts.append({
                "source": source["name"],
                "version": version,
                "winutils": f"{base_url}/winutils.exe",
                "hadoop_dll": f"{base_url}/hadoop.dll"
            })
    return attempts

def download_file(url, destination):
    """Try to download a file from URL to destination"""
    try:
        urllib.request.urlretrieve(url, destination)
        # Verify the file was actually downloaded and has content
        if os.path.exists(destination) and os.path.getsize(destination) > 0:
            return True
        else:
            os.remove(destination) if os.path.exists(destination) else None
            return False
    except Exception as e:
        # Clean up partial download
        if os.path.exists(destination):
            os.remove(destination)
        return False

def setup_hadoop_windows():
    # Create hadoop/bin directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hadoop_home = os.path.join(script_dir, "hadoop")
    bin_dir = os.path.join(hadoop_home, "bin")
    
    os.makedirs(bin_dir, exist_ok=True)
    
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")
    
    print("=" * 60)
    print("Setting up Hadoop for Windows (PySpark compatibility)")
    print("=" * 60)
    
    # Download winutils.exe if not present
    if not os.path.exists(winutils_path):
        print(f"\nDownloading winutils.exe...")
        print("Trying multiple sources and versions...\n")
        
        success = False
        attempts = get_download_attempts()
        
        for i, attempt in enumerate(attempts, 1):
            print(f"[{i}/{len(attempts)}] Trying {attempt['source']} - Hadoop {attempt['version']}")
            print(f"  URL: {attempt['winutils']}")
            
            if download_file(attempt["winutils"], winutils_path):
                print(f"  ✓ Successfully downloaded winutils.exe!")
                success = True
                
                # Also try to download hadoop.dll with same version
                if not os.path.exists(hadoop_dll_path):
                    print(f"  Downloading hadoop.dll...")
                    if download_file(attempt["hadoop_dll"], hadoop_dll_path):
                        print(f"  ✓ Successfully downloaded hadoop.dll")
                    else:
                        print(f"  ⚠ Could not download hadoop.dll (optional)")
                
                print(f"\n✓ Setup successful with {attempt['source']} - Hadoop {attempt['version']}")
                break
            else:
                print(f"  ✗ Failed")
        
        if not success:
            print("\n" + "=" * 60)
            print("❌ AUTOMATIC DOWNLOAD FAILED")
            print("=" * 60)
            print("\n📥 MANUAL DOWNLOAD REQUIRED:")
            print("\n1. Visit ONE of these repositories:")
            print("   • https://github.com/kontext-tech/winutils (RECOMMENDED)")
            print("   • https://github.com/steveloughran/winutils")
            print("   • https://github.com/cdarlint/winutils")
            print("\n2. Navigate to: hadoop-3.3.1/bin (or any 3.x version)")
            print("\n3. Download these files:")
            print("   • winutils.exe")
            print("   • hadoop.dll")
            print(f"\n4. Save them to this directory:")
            print(f"   {bin_dir}")
            print("\n5. After downloading, verify with:")
            print(f"   dir \"{bin_dir}\"")
            print("\n" + "=" * 60)
            return False
    else:
        print(f"\n✓ winutils.exe already exists")
        print(f"  Location: {winutils_path}")
        print(f"  Size: {os.path.getsize(winutils_path):,} bytes")
    
    # Check hadoop.dll
    if os.path.exists(hadoop_dll_path):
        print(f"\n✓ hadoop.dll exists")
        print(f"  Location: {hadoop_dll_path}")
        print(f"  Size: {os.path.getsize(hadoop_dll_path):,} bytes")
    else:
        print(f"\n⚠ hadoop.dll not found (optional but recommended)")
    
    # Display environment info
    print("\n" + "=" * 60)
    print("✅ SETUP COMPLETE")
    print("=" * 60)
    print(f"\nHADOOP_HOME: {hadoop_home}")
    print(f"Binary path: {bin_dir}")
    print("\nThe spark_processor.py will automatically configure this.")
    print("\n🚀 Next step: Run your Spark processor")
    print("   python src/spark_processor.py")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = setup_hadoop_windows()
    sys.exit(0 if success else 1)
