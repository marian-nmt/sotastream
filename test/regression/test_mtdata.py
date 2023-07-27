import sys
import subprocess
import logging as log
import pytest

log.getLogger(name=__name__).setLevel(log.INFO)


EXPECTED = """\
ಮತ್ತು ಜನಸಂಖ್ಯಾ ಸ್ಥಿರತಾ ಕೋಶ (ಜೆ.ಎಸ್.ಕೆ.)ಯನ್ನು ಮುಚ್ಚುವ ಮತ್ತು ಈ ಕಾರ್ಯಗಳನ್ನು ಆರೋಗ್ಯ ಮತ್ತು ಕುಟುಂಬ ಕಲ್ಯಾಣ ಇಲಾಖೆ (ಡಿಓಎಚ್ಎಫ್.ಡಬ್ಲು) ನಲ್ಲಿ ನಿಯೋಜಿಸಲು ಉದ್ದೇಶಿಸಿರುವ ಪ್ರಸ್ತಾಪಕ್ಕೆ ತನ್ನ ಅನುಮೋದನೆ ನೀಡಿದೆ.	The Union Cabinet chaired by Prime Minister Shri Narendra Modi has approved the proposal for closure of Autonomous Bodies, namely, Rashtriya Arogya Nidhi (RAN) and Jansankhya Sthirata Kosh (JSK) and the functions are proposed to be vested in Department of Health & Family Welfare (DoHFW).
ಆರೋಗ್ಯ ಮತ್ತು ಕುಟುಂಬ ಕಲ್ಯಾಣ ಇಲಾಖೆಯ ಅಡಿಯಲ್ಲಿರುವ ಸ್ವಾಯತ್ತ ಕಾಯಗಳ ತರ್ಕಬದ್ಧೀಕರಣವು ಈ ಕಾಯಗಳ ಚಾಲ್ತಿಯಲ್ಲಿರುವ ಅಂಗರಚನೆಯಂತೆ ಅಂತರ ಸಚಿವಾಲಯದ ಸಮಾಲೋಚನೆ ಮತ್ತು ಪರಾಮರ್ಶೆಯನ್ನು ಒಳಗೊಂಡಿರುತ್ತದೆ.	The rationalization of Autonomous Bodies under Department of Health & Family Welfare will involve inter-ministerial consultations and review of existing bye laws of these bodies.
Steigt Gold auf 10.000 Dollar?	$10,000 Gold?
ಇದರ ಜಾರಿಗೆ ಕಾಲಮಿತಿ ಒಂದು ವರ್ಷವಾಗಿರುತ್ತದೆ.	The time frame for implementation is one year,
ನಿಯೋಜಿತ ಕೇಂದ್ರ ಸರ್ಕಾರದ ಆಸ್ಪತ್ರೆಗಳಲ್ಲಿ ಬಡ ರೋಗಿಗಳು ಪಡೆಯುವ ಚಿಕಿತ್ಸೆಗೆ ವೈದ್ಯಕೀಯ ನೆರವು ಒದಗಿಸಲು ರಾಷ್ಟ್ರೀಯ ಆರೋಗ್ಯ ನಿಧಿ (ಆರ್.ಎ.ಎನ್) ಯನ್ನು ನೋಂದಾಯಿತ ಸೊಸೈಟಿಯಾಗಿ ಸ್ಥಾಪಿಸಲಾಗಿದೆ.	Rashtriya Arogya Nidhi (RAN) was set up as a registered society to provide financial medical assistance to poor patients receiving treatment in designated central government hospitals.
ಪ್ರಕರಣಗಳ ಆಧಾರದ ಮೇಲೆ ನೆರವು ಒದಗಿಸಲು ಅಂಥ ಆಸ್ಪತ್ರೆಗಳ ವೈದ್ಯಕೀಯ ಸೂಪರಿಂಟೆಂಡೆಂಟ್ ಗಳ ಬಳಿ ಮುಂಗಡವನ್ನು ಇಡಲಾಗಿದೆ.	An advance is placed with the Medical Superintendents of such hospitals who then provide assistance on a case to case basis.
ಡಿಓಎಚ್.ಎಫ್.ಡಬ್ಲ್ಯು. ಆಸ್ಪತ್ರೆಗಳಿಗೆ ನಿಧಿ ಒದಗಿಸುವುದರಿಂದ, ಸಹಾಯಧನವನ್ನು ಇಲಾಖೆಯಿಂದ ನೇರವಾಗಿ ಆಸ್ಪತ್ರೆಗಳಿಗೇ ನೀಡಲಾಗುತ್ತಿದೆ.	Since the DoHFW provides funds to the hospitals, the grants can be given from the Department to the hospital directly.
ಆರ್.ಎ.ಎನ್ ಸೊಸೈಟಿಯ ಆಡಳಿತ ಮಂಡಳಿಗಳು ಸೊಸೈಟಿಗಳ ನೋಂದಣಿ ಕಾಯಿದೆ, 1860 (ಎಸ್.ಆರ್.ಎ) ಯ ನಿಬಂಧನೆಗಳ ಪ್ರಕಾರ ಸ್ವಾಯತ್ತ ಕಾಯವಾಗಿ (ಎಬಿ) ವಿಸರ್ಜಿಸಬಹುದಾಗಿರುತ್ತದೆ.	Managing Committee of RAN Society will meet to dissolve the Autonomous Body (AB) as per provisions of Societies Registration Act, 1860 (SRA).
SAN FRANCISCO – Es war noch nie leicht, ein rationales Gespräch über den Wert von Gold zu führen.	SAN FRANCISCO – It has never been easy to have a rational conversation about the value of gold.
ಇದರ ಜೊತೆಗೆ ಆರೋಗ್ಯ ಸಚಿವಾಲಯದ ಕ್ಯಾನ್ಸರ್ ರೋಗಿಗಳ ನಿಧಿ (ಎಚ್.ಎಂ.ಸಿ.ಪಿ.ಎಫ್.)ಯನ್ನು ಕೂಡ ಇಲಾಖೆಗೆ ವರ್ಗಾಯಿಸಲಾಗುತ್ತದೆ.	In addition to this, Health Minister’s Cancer Patient Fund (HMCPF) shall also be transferred to the Department."""


def test_mtdata():
    """
    Test MTData pipeline.
    Starts a subprocess and reads its output.
    """

    try:
        from mtdata.data import INDEX

        print("Import successful")
    except ImportError:
        pytest.skip("mtdata is unavailable")

    base_cmd = f'{sys.executable} -m sotastream -n 1 -q 1000 -b 1000 --seed 43'
    cmd = f'{base_cmd} mtdata -lp mul-eng Statmt-news_commentary-16-deu-eng Statmt-pmindia-1-eng-kan --mix-weights 1 2'
    log.info(f'Running command: {cmd}')
    proc = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    try:
        expected = EXPECTED.split('\n')
        recieved = []
        i = 0
        for line in proc.stdout:
            recieved.append(line.rstrip('\n'))
            i += 1
            if i >= len(expected):
                break
        assert recieved == expected
    finally:
        proc.terminate()
